package org.janelia.jacs2.asyncservice.common;

import java.util.Map;
import java.util.function.Function;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.dataservice.notifservice.EmailNotificationService;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsNotificationDao;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.service.JacsNotification;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEvent;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceLifecycleStage;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.RegisteredJacsNotification;
import org.slf4j.Logger;
import org.slf4j.MDC;

@MdcContext
@ApplicationScoped
public class JacsServiceDispatcher {

    private static final int DISPATCH_BATCH_SIZE = 20;

    private ServiceComputationFactory serviceComputationFactory;
    private JacsServiceQueue jacsServiceQueue;
    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private JacsNotificationDao jacsNotificationDao;
    private JacsServiceEngine jacsServiceEngine;
    private EmailNotificationService emailNotificationService;
    private Logger logger;

    JacsServiceDispatcher() {
        // CDI required ctor
    }

    @Inject
    public JacsServiceDispatcher(ServiceComputationFactory serviceComputationFactory,
                                 JacsServiceQueue jacsServiceQueue,
                                 JacsServiceDataPersistence jacsServiceDataPersistence,
                                 JacsNotificationDao jacsNotificationDao,
                                 JacsServiceEngine jacsServiceEngine,
                                 EmailNotificationService emailNotificationService,
                                 Logger logger) {
        this.serviceComputationFactory = serviceComputationFactory;
        this.jacsServiceQueue = jacsServiceQueue;
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
        this.jacsNotificationDao = jacsNotificationDao;
        this.jacsServiceEngine = jacsServiceEngine;
        this.emailNotificationService = emailNotificationService;
        this.logger = logger;
    }

    void dispatchServices() {
        for (int i = 0; i < DISPATCH_BATCH_SIZE; i++) {
            JacsServiceData queuedService = jacsServiceQueue.dequeService();
            if (queuedService == null) {
                // nothing to do
                return;
            }

            logger.debug("Dequeued service {}", queuedService);

            if (!queuedService.hasParentServiceId()) {
                // if this is a root service, i.e. no other currently running service depends on it
                // then try to acquire a slot otherwise let this pass through
                if (!jacsServiceEngine.acquireSlot()) {
                    logger.debug("Abort service {} for now because there are not enough processing slots", queuedService);
                    jacsServiceQueue.abortService(queuedService);
                    continue; // no slot available
                }
            }
            dispatchService(queuedService);
        }
    }

    @SuppressWarnings("unchecked")
    private void dispatchService(JacsServiceData jacsServiceData) {
        logger.debug("Dispatch service {}", jacsServiceData);

        try {
            ServiceProcessor<?> serviceProcessor = jacsServiceEngine.getServiceProcessor(jacsServiceData);
            JacsServiceState initialServiceState = jacsServiceData.getState();
            jacsServiceDataPersistence.updateServiceState(jacsServiceData, JacsServiceState.DISPATCHED, JacsServiceEvent.NO_EVENT);
            serviceComputationFactory.newCompletedComputation(jacsServiceData)
                    .thenSuspendUntil(new WaitingForDependenciesContinuationCond<>(
                            Function.identity(),
                            (JacsServiceData sd, JacsServiceData tmpSd) -> tmpSd,
                            jacsServiceDataPersistence,
                            logger).negate())
                    .thenApply((JacsServiceData sd) -> {
                        Map<String, EntityFieldValueHandler<?>> sdUpdates = null;
                        ServiceArgsHandler serviceArgsHandler = new ServiceArgsHandler(jacsServiceDataPersistence);
                        if (initialServiceState == JacsServiceState.QUEUED) {
                            sdUpdates = serviceArgsHandler.updateServiceArgs(serviceProcessor.getMetadata(), sd);
                            sendNotification(sd, JacsServiceLifecycleStage.START_PROCESSING);
                        } else if (initialServiceState == JacsServiceState.RESUMED) {
                            sdUpdates = serviceArgsHandler.updateServiceArgs(serviceProcessor.getMetadata(), sd);
                            sendNotification(sd, JacsServiceLifecycleStage.RESUME_PROCESSING);
                        } else {
                            // assume that since it was processed once the service args were handled already
                            sendNotification(sd, JacsServiceLifecycleStage.RETRY_PROCESSING);
                        }
                        logger.debug("Update service args for {} to {}", sd, sdUpdates);
                        jacsServiceDataPersistence.update(sd, sdUpdates);
                        return sd;
                    })
                    .thenCompose((JacsServiceData sd) -> serviceProcessor.process(sd))
                    .thenApply(r -> {
                        success(r.getJacsServiceData());
                        return r;
                    })
                    .exceptionally((Throwable exc) -> JacsServiceResult.class.cast(handleException(jacsServiceData, exc)))
                    .whenComplete((r, exc) -> {
                        serviceFinally(jacsServiceData);
                    });
        } catch (Throwable e) {
            handleException(jacsServiceData, e);
            serviceFinally(jacsServiceData);
        }
    }

    private void serviceFinally(JacsServiceData jacsServiceData) {
        jacsServiceQueue.completeService(jacsServiceData);
        if (!jacsServiceData.hasParentServiceId()) {
            // release the slot acquired before the service was started
            jacsServiceEngine.releaseSlot();
        }
    }

    private void success(JacsServiceData serviceData) {
        MDC.put("serviceName", serviceData.getName());
        MDC.put("serviceId", serviceData.getServiceId());
        if (logger.isDebugEnabled()) {
            logger.debug("Processing successful {}", serviceData);
        } else {
            logger.info("Processing successful {}", serviceData.getShortName());
        }

        JacsServiceData latestServiceData = jacsServiceDataPersistence.findById(serviceData.getId());
        if (latestServiceData == null) {
            logger.warn("No Service not found for {} - probably it was already archived", serviceData);
            return;
        }
        if (latestServiceData.hasCompletedSuccessfully()) {
            // nothing to do
            logger.debug("Service {} has already been marked as successful", latestServiceData);
        } else {
            jacsServiceDataPersistence.updateServiceState(
                    latestServiceData,
                    JacsServiceState.SUCCESSFUL,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.COMPLETED, "Completed successfully"));
        }
        sendNotification(latestServiceData, JacsServiceLifecycleStage.SUCCESSFUL_PROCESSING);
        emailNotificationService.sendNotification(
                String.format("Service %s#%d completed successfully", serviceData.getName(), serviceData.getId()),
                String.format("Service %s#%d - %s completed successfully", serviceData.getName(), serviceData.getId(), serviceData.getArgs()),
                serviceData.getSuccessEmailNotifications()
        );

        if (!latestServiceData.hasParentServiceId()) {
            finalizeRootService(latestServiceData);
        }
    }

    private JacsServiceResult<Throwable> handleException(JacsServiceData serviceData, Throwable exc) {
        MDC.put("serviceName", serviceData.getName());
        MDC.put("serviceId", serviceData.getServiceId());
        if (logger.isDebugEnabled()) {
            logger.error("Processing error executing {}", serviceData, exc);
        } else {
            logger.error("Processing error executing {}", serviceData.getShortName(), exc);
        }

        JacsServiceData latestServiceData = jacsServiceDataPersistence.findById(serviceData.getId());
        if (latestServiceData == null) {
            logger.warn("No Service not found for {}", serviceData.getId());
            return new JacsServiceResult<>(serviceData, exc);
        }
        if (latestServiceData.hasBeenSuspended()) {
            sendNotification(latestServiceData, JacsServiceLifecycleStage.SUSPEND_PROCESSING);
        } else  if (exc instanceof ServiceSuspendedException) {
            if (!latestServiceData.hasCompleted()) {
                // in this case only suspend it if it has not been completed
                jacsServiceDataPersistence.updateServiceState(latestServiceData, JacsServiceState.SUSPENDED, JacsServiceEvent.NO_EVENT);
                sendNotification(latestServiceData, JacsServiceLifecycleStage.SUSPEND_PROCESSING);
                emailNotificationService.sendNotification(
                        String.format("Service %s#%d processing has been suspended %s", serviceData.getName(), serviceData.getId(), serviceData.getState()),
                        String.format("Service %s#%d - %s has been suspended %s", serviceData.getName(), serviceData.getId(), serviceData.getArgs(), serviceData.getState()),
                        serviceData.getFailureEmailNotifications()
                );
            }
        } else {
            if (latestServiceData.hasCompletedUnsuccessfully()) {
                // nothing to do
                logger.debug("Service {} has already been marked as failed", latestServiceData);
            } else {
                jacsServiceDataPersistence.updateServiceState(
                        latestServiceData,
                        JacsServiceState.ERROR,
                        JacsServiceData.createServiceEvent(JacsServiceEventTypes.FAILED, String.format("Failed: %s", exc.getMessage())));
            }
            sendNotification(latestServiceData, JacsServiceLifecycleStage.FAILED_PROCESSING);
            emailNotificationService.sendNotification(
                    String.format("Service %s#%d processing %s", serviceData.getName(), serviceData.getId(), serviceData.getState()),
                    String.format("Service %s#%d - %s has failed: %s", serviceData.getName(), serviceData.getId(), serviceData.getArgs(), serviceData.getState()),
                    serviceData.getFailureEmailNotifications()
            );
        }
        return new JacsServiceResult<>(serviceData, exc);
    }

    private void sendNotification(JacsServiceData sd, JacsServiceLifecycleStage lifecycleStage) {
        if (sd.getProcessingNotification() != null && sd.getProcessingNotification().getRegisteredLifecycleStages().contains(lifecycleStage)) {
            createNotification(sd, lifecycleStage, sd.getProcessingNotification());
        }
    }

    private void createNotification(JacsServiceData sd, JacsServiceLifecycleStage lifecycleStage, RegisteredJacsNotification rn) {
        JacsNotification jacsNotification = new JacsNotification();
        jacsNotification.setEventName(rn.getEventName());
        jacsNotification.setNotificationData(rn.getNotificationData());
        jacsNotification.setNotificationStage(lifecycleStage);
        jacsNotification.addNotificationData("args", sd.getArgs().toString());
        logger.info("Service {} -> {}", sd, jacsNotification);
        jacsNotificationDao.save(jacsNotification);
    }

    private void finalizeRootService(JacsServiceData sd) {
        logger.info("Finished {}", sd);
    }

}

package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.model.access.dao.JacsNotificationDao;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.*;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Optional;
import java.util.function.Function;

@ApplicationScoped
public class JacsServiceDispatcher {

    private static final int DISPATCH_BATCH_SIZE = 20;

    private ServiceComputationFactory serviceComputationFactory;
    private JacsServiceQueue jacsServiceQueue;
    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private JacsNotificationDao jacsNotificationDao;
    private JacsServiceEngine jacsServiceEngine;
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
                                 Logger logger) {
        this.serviceComputationFactory = serviceComputationFactory;
        this.jacsServiceQueue = jacsServiceQueue;
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
        this.jacsNotificationDao = jacsNotificationDao;
        this.jacsServiceEngine = jacsServiceEngine;
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
                    logger.info("Abort service {} for now because there are not enough processing slots", queuedService);
                    jacsServiceQueue.abortService(queuedService);
                    continue; // no slot available
                }
            }
            dispatchService(queuedService);
        }
    }

    @MdcContext
    private void dispatchService(JacsServiceData jacsServiceData) {
        logger.info("Dispatch service {}", jacsServiceData);
        try {
            ServiceProcessor<?> serviceProcessor = jacsServiceEngine.getServiceProcessor(jacsServiceData);
            jacsServiceDataPersistence.updateServiceState(jacsServiceData, JacsServiceState.DISPATCHED, Optional.empty());
            serviceComputationFactory.newCompletedComputation(jacsServiceData)
                    .thenSuspendUntil(new SuspendServiceContinuationCond<>(
                            Function.identity(),
                            (JacsServiceData sd, JacsServiceData tmpSd) -> tmpSd,
                            jacsServiceDataPersistence,
                            logger).negate())
                    .thenApply((JacsServiceData sd) -> {
                        sendNotification(jacsServiceData, JacsServiceLifecycleStage.START_PROCESSING);
                        return sd;
                    })
                    .thenCompose(sd -> serviceProcessor.process(sd))
                    .thenApply(r -> {
                        success(r.getJacsServiceData());
                        return r;
                    })
                    .exceptionally(exc -> {
                        fail(jacsServiceData, exc);
                        throw new ComputationException(jacsServiceData, exc);
                    })
                    .whenComplete((r, exc) -> {
                        serviceFinally(jacsServiceData);
                    })
            ;
        } catch (Throwable e) {
            fail(jacsServiceData, e);
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

    @MdcContext
    private void success(JacsServiceData serviceData) {
        logger.info("Processing successful {}", serviceData);
        JacsServiceData latestServiceData = jacsServiceDataPersistence.findById(serviceData.getId());
        if (latestServiceData == null) {
            logger.warn("No Service not found for {} - probably it was already archived", serviceData);
            return;
        }
        if (latestServiceData.hasCompletedUnsuccessfully()) {
            logger.warn("Attempted to overwrite failed state with success for {}", latestServiceData);
        }
        if (latestServiceData.hasCompletedSuccessfully()) {
            // nothing to do
            logger.debug("Service {} has already been marked as successful", latestServiceData);
        } else {
            jacsServiceDataPersistence.updateServiceState(
                    latestServiceData,
                    JacsServiceState.SUCCESSFUL,
                    Optional.of(JacsServiceData.createServiceEvent(JacsServiceEventTypes.COMPLETED, "Completed successfully")));
        }
        sendNotification(latestServiceData, JacsServiceLifecycleStage.SUCCESSFUL_PROCESSING);
        if (!latestServiceData.hasParentServiceId()) {
            archiveServiceData(latestServiceData.getId());
        }
    }

    @MdcContext
    private void fail(JacsServiceData serviceData, Throwable exc) {
        logger.error("Processing error executing {}", serviceData, exc);
        JacsServiceData latestServiceData = jacsServiceDataPersistence.findById(serviceData.getId());
        if (latestServiceData == null) {
            logger.warn("NO Service not found for {} - probably it was already archived", serviceData);
            return;
        }
        if (latestServiceData.hasCompletedSuccessfully()) {
            logger.warn("Service {} has failed after has already been markes as successfully completed", latestServiceData);
        }
        if (latestServiceData.hasCompletedUnsuccessfully()) {
            // nothing to do
            logger.debug("Service {} has already been marked as failed", latestServiceData);
        } else {
            jacsServiceDataPersistence.updateServiceState(
                    latestServiceData,
                    JacsServiceState.ERROR,
                    Optional.of(JacsServiceData.createServiceEvent(JacsServiceEventTypes.FAILED, String.format("Failed: %s", exc.getMessage()))));
        }
        sendNotification(latestServiceData, JacsServiceLifecycleStage.FAILED_PROCESSING);
    }

    private void sendNotification(JacsServiceData sd, JacsServiceLifecycleStage lifecycleStage) {
        if (sd.getProcessingNotification() != null && sd.getProcessingNotification().getRegisteredLifecycleStages().contains(lifecycleStage)) {
            sendNotification(sd, lifecycleStage, sd.getProcessingNotification());
        }
    }

    private void sendNotification(JacsServiceData sd, JacsServiceLifecycleStage lifecycleStage, RegisteredJacsNotification rn) {
        JacsNotification jacsNotification = new JacsNotification();
        jacsNotification.setEventName(rn.getEventName());
        jacsNotification.setNotificationData(rn.getNotificationData());
        jacsNotification.setNotificationStage(lifecycleStage);
        jacsNotification.getNotificationData().put("args", sd.getArgs().toString());
        logger.info("Service {} -> {}", sd, jacsNotification);
        jacsNotificationDao.save(jacsNotification);
    }

    private void archiveServiceData(Number serviceId) {
        JacsServiceData jacsServiceDataHierarchy = jacsServiceDataPersistence.findServiceHierarchy(serviceId);
        jacsServiceDataPersistence.archiveHierarchy(jacsServiceDataHierarchy);
    }

}

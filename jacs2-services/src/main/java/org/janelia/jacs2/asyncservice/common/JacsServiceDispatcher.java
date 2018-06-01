package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsNotificationDao;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.service.*;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;
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
                    logger.debug("Abort service {} for now because there are not enough processing slots", queuedService);
                    jacsServiceQueue.abortService(queuedService);
                    continue; // no slot available
                }
            }
            dispatchService(queuedService);
        }
    }

    @MdcContext
    private void dispatchService(JacsServiceData jacsServiceData) {
        logger.debug("Dispatch service {}", jacsServiceData);

        try {
            ServiceProcessor<?> serviceProcessor = jacsServiceEngine.getServiceProcessor(jacsServiceData);
            JacsServiceState initialServiceState = jacsServiceData.getState();
            jacsServiceDataPersistence.updateServiceState(jacsServiceData, JacsServiceState.DISPATCHED, JacsServiceEvent.NO_EVENT);
            serviceComputationFactory.newCompletedComputation(jacsServiceData)
                    .thenApply(sd -> {
                        JacsServiceData refreshedSd = refresh(jacsServiceData);
                        invokeInterceptors(refreshedSd, interceptor -> interceptor.onDispatch(refreshedSd));
                        return refreshedSd;
                    })
                    .thenSuspendUntil(new WaitingForDependenciesContinuationCond<>(
                            Function.identity(),
                            (JacsServiceData sd, JacsServiceData tmpSd) -> tmpSd,
                            jacsServiceDataPersistence,
                            logger).negate())
                    .thenApply((JacsServiceData sd) -> {

                        if (initialServiceState == JacsServiceState.QUEUED)
                            sendNotification(sd, JacsServiceLifecycleStage.START_PROCESSING);
                        else if (initialServiceState == JacsServiceState.RESUMED)
                            sendNotification(sd, JacsServiceLifecycleStage.RESUME_PROCESSING);
                        else
                            sendNotification(sd, JacsServiceLifecycleStage.RETRY_PROCESSING);

                        ServiceArgsHandler serviceArgsHandler = new ServiceArgsHandler(jacsServiceDataPersistence);
                        Map<String, EntityFieldValueHandler<?>> sdUpdates =
                                serviceArgsHandler.updateServiceArgs(serviceProcessor.getMetadata(), sd);
                        logger.debug("Update service args for {} to {}", sd, sdUpdates);
                        jacsServiceDataPersistence.update(sd, sdUpdates);
                        JacsServiceData refreshedSd = refresh(jacsServiceData);
                        invokeInterceptors(refreshedSd, interceptor -> interceptor.beforeProcess(refreshedSd));
                        return refreshedSd;
                    })
                    .thenCompose((JacsServiceData sd) -> {
                        JacsServiceData refreshedSd = refresh(jacsServiceData);
                        return serviceProcessor.process(refreshedSd);
                    })
                    .thenApply(r -> {
                        JacsServiceData refreshedSd = refresh(jacsServiceData);
                        invokeInterceptors(refreshedSd, interceptor -> interceptor.afterProcess(refreshedSd));
                        return refreshedSd;
                    })
                    .whenComplete((r, exc) -> {
                        JacsServiceData sd = refresh(jacsServiceData);
                        if (exc != null) {
                            handleException(sd, exc);
                        }
                        else {
                            success(sd);
                        }
                        // We have to refresh again, because the methods above updated the service state
                        serviceFinally(refresh(jacsServiceData));
                    });
        }
        catch (Throwable e) {
            handleException(refresh(jacsServiceData), e);
            serviceFinally(refresh(jacsServiceData));
        }
    }

    private void serviceFinally(JacsServiceData sd) {
        jacsServiceQueue.completeService(sd);
        if (!sd.hasParentServiceId()) {
            // release the slot acquired before the service was started
            jacsServiceEngine.releaseSlot();
        }
        JacsServiceData refreshedSd = refresh(sd);
        invokeInterceptors(refreshedSd, interceptor -> interceptor.andFinally(refreshedSd));
    }

    private void invokeInterceptors(JacsServiceData sd, Consumer<ServiceInterceptor> consumer) {
        if (sd==null) return;
        for (ServiceInterceptor serviceInterceptor : jacsServiceEngine.getServiceInterceptors(sd)) {
            consumer.accept(serviceInterceptor);
        }
    }

    @MdcContext
    private void success(JacsServiceData sd) {

        if (logger.isDebugEnabled()) {
            logger.debug("Processing successful {}", sd);
        }
        else {
            logger.info("Processing successful {}", sd.getShortName());
        }

        if (sd.hasCompletedUnsuccessfully()) {
            logger.warn("Attempted to overwrite failed state with success for {}", sd);
        }
        if (sd.hasCompletedSuccessfully()) {
            // nothing to do
            logger.debug("Service {} has already been marked as successful", sd);
        }
        else {
            jacsServiceDataPersistence.updateServiceState(
                    sd,
                    JacsServiceState.SUCCESSFUL,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.COMPLETED, "Completed successfully"));
        }
        sendNotification(sd, JacsServiceLifecycleStage.SUCCESSFUL_PROCESSING);
        if (!sd.hasParentServiceId()) {
            finalizeRootService(sd);
        }
    }

    @MdcContext
    private JacsServiceResult<Throwable> handleException(JacsServiceData sd, Throwable exc) {

        if (exc instanceof CancellationException) {
            logger.info("Service was cancelled: {}", sd.getShortName());
            return new JacsServiceResult<>(sd, exc);
        }

        if (logger.isDebugEnabled()) {
            logger.error("Processing error executing {}", sd, exc);
        } else {
            logger.error("Processing error executing {}", sd.getShortName(), exc);
        }

        if (sd == null) {
            logger.warn("No Service not found for {}", sd.getId());
            return new JacsServiceResult<>(sd, exc);
        }
        if (sd.hasBeenSuspended()) {
            sendNotification(sd, JacsServiceLifecycleStage.SUSPEND_PROCESSING);
        } else  if (exc instanceof ServiceSuspendedException) {
            if (!sd.hasCompleted()) {
                // in this case only suspend it if it has not been completed
                jacsServiceDataPersistence.updateServiceState(sd, JacsServiceState.SUSPENDED, JacsServiceEvent.NO_EVENT);
                sendNotification(sd, JacsServiceLifecycleStage.SUSPEND_PROCESSING);
            }
        } else {
            if (sd.hasCompletedSuccessfully()) {
                logger.warn("Service {} has failed after has already been marked as successful", sd);
            }
            if (sd.hasCompletedUnsuccessfully()) {
                // nothing to do
                logger.debug("Service {} has already been marked as failed", sd);
            } else {
                jacsServiceDataPersistence.updateServiceState(
                        sd,
                        JacsServiceState.ERROR,
                        JacsServiceData.createServiceEvent(JacsServiceEventTypes.FAILED, String.format("Failed: %s", exc.getMessage())));
            }
            sendNotification(sd, JacsServiceLifecycleStage.FAILED_PROCESSING);
        }
        return new JacsServiceResult<>(sd, exc);
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

    private void finalizeRootService(JacsServiceData sd) {
        logger.info("Finished {}", sd);
    }

    private JacsServiceData refresh(JacsServiceData sd) {
        JacsServiceData refreshedSd = jacsServiceDataPersistence.findById(sd.getId());
        if (refreshedSd == null) {
            throw new IllegalStateException("Service data not found for "+sd.getId());
        }
        return refreshedSd;
    }

}

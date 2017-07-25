package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.dao.JacsNotificationDao;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsNotification;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceLifecycleStage;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.janelia.jacs2.model.jacsservice.RegisteredJacsNotification;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Optional;

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
        ServiceProcessor<?> serviceProcessor = jacsServiceEngine.getServiceProcessor(jacsServiceData);
        serviceComputationFactory.<JacsServiceData>newComputation()
                .supply(() -> {
                    sendNotification(jacsServiceData, JacsServiceLifecycleStage.START_PROCESSING);
                    jacsServiceDataPersistence.updateServiceState(jacsServiceData, JacsServiceState.SUBMITTED, Optional.empty());
                    return jacsServiceData;
                })
                .thenCompose(sd -> serviceProcessor.process(sd))
                .thenApply(r -> {
                    JacsServiceData service = jacsServiceDataPersistence.findById(jacsServiceData.getId());
                    success(service);
                    return r;
                })
                .exceptionally(exc -> {
                    JacsServiceData service = jacsServiceDataPersistence.findById(jacsServiceData.getId());
                    fail(service, exc);
                    throw new ComputationException(service, exc);
                })
                .whenComplete((r, exc) -> {
                    jacsServiceQueue.completeService(jacsServiceData);
                    if (!jacsServiceData.hasParentServiceId()) {
                        // release the slot acquired before the service was started
                        jacsServiceEngine.releaseSlot();
                    }
                })
        ;
    }

    @MdcContext
    private void success(JacsServiceData jacsServiceData) {
        logger.info("Processing successful {}", jacsServiceData);
        if (jacsServiceData.hasCompletedUnsuccessfully()) {
            logger.warn("Attempted to overwrite failed state with success for {}", jacsServiceData);
        }
        if (jacsServiceData.hasCompletedSuccessfully()) {
            // nothing to do
            logger.debug("Service {} has already been marked as successful", jacsServiceData);
        } else {
            jacsServiceDataPersistence.updateServiceState(
                    jacsServiceData,
                    JacsServiceState.SUCCESSFUL,
                    Optional.of(JacsServiceData.createServiceEvent(JacsServiceEventTypes.COMPLETED, "Completed successfully")));
        }
        sendNotification(jacsServiceData, JacsServiceLifecycleStage.SUCCESSFUL_PROCESSING);
        if (!jacsServiceData.hasParentServiceId()) {
            archiveServiceData(jacsServiceData.getId());
        }
    }

    @MdcContext
    private void fail(JacsServiceData jacsServiceData, Throwable exc) {
        logger.error("Processing error executing {}", jacsServiceData, exc);
        if (jacsServiceData.hasCompletedSuccessfully()) {
            logger.warn("Service {} has failed after has already been markes as successfully completed", jacsServiceData);
        }
        if (jacsServiceData.hasCompletedUnsuccessfully()) {
            // nothing to do
            logger.debug("Service {} has already been marked as failed", jacsServiceData);
        } else {
            jacsServiceDataPersistence.updateServiceState(
                    jacsServiceData,
                    JacsServiceState.ERROR,
                    Optional.of(JacsServiceData.createServiceEvent(JacsServiceEventTypes.FAILED, String.format("Failed: %s", exc.getMessage()))));
        }
        sendNotification(jacsServiceData, JacsServiceLifecycleStage.FAILED_PROCESSING);
   }

    private void sendNotification(JacsServiceData sd, JacsServiceLifecycleStage lifecycleStage) {
        if (sd.getProcessingNotification() != null && sd.getProcessingNotification().getRegisteredLifecycleStages().contains(lifecycleStage)) {
            sendNotification(sd, lifecycleStage, sd.getProcessingNotification());
        }
    }

    private void sendNotification(JacsServiceData sd, JacsServiceLifecycleStage lifecycleStage, RegisteredJacsNotification rn) {
        logger.info("Service {} - stage {}: {}", sd, lifecycleStage, rn);
        JacsNotification jacsNotification = new JacsNotification();
        jacsNotification.setNotificationData(rn.getNotificationData());
        jacsNotification.setNotificationStage(lifecycleStage);
        jacsNotificationDao.save(jacsNotification);
    }

    private void archiveServiceData(Number serviceId) {
        JacsServiceData jacsServiceDataHierarchy = jacsServiceDataPersistence.findServiceHierarchy(serviceId);
        jacsServiceDataPersistence.archiveHierarchy(jacsServiceDataHierarchy);
    }

}

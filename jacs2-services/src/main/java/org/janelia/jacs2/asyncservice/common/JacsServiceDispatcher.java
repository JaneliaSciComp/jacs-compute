package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class JacsServiceDispatcher {

    private static final int DISPATCH_BATCH_SIZE = 20;

    private final ServiceComputationFactory serviceComputationFactory;
    private final JacsServiceQueue jacsServiceQueue;
    private final JacsServiceDataPersistence jacsServiceDataPersistence;
    private final JacsServiceEngine jacsServiceEngine;
    private final Logger logger;

    @Inject
    public JacsServiceDispatcher(ServiceComputationFactory serviceComputationFactory,
                                 JacsServiceQueue jacsServiceQueue,
                                 JacsServiceDataPersistence jacsServiceDataPersistence,
                                 JacsServiceEngine jacsServiceEngine,
                                 Logger logger) {
        this.serviceComputationFactory = serviceComputationFactory;
        this.jacsServiceQueue = jacsServiceQueue;
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
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
            logger.info("Dispatch service {}", queuedService);
            ServiceProcessor<?> serviceProcessor = jacsServiceEngine.getServiceProcessor(queuedService);
            serviceComputationFactory.<JacsServiceData>newComputation()
                    .supply(() -> {
                        logger.debug("Submit {}", queuedService);
                        queuedService.updateState(JacsServiceState.SUBMITTED);
                        updateServiceData(queuedService);
                        return queuedService;
                    })
                    .thenCompose(sd -> serviceProcessor.process(sd))
                    .exceptionally(exc -> {
                        JacsServiceData service = jacsServiceDataPersistence.findById(queuedService.getId());
                        fail(service, exc);
                        throw new ComputationException(service, exc);
                    })
                    .whenComplete((r, exc) -> {
                        JacsServiceData service = jacsServiceDataPersistence.findById(queuedService.getId());
                        if (!service.hasParentServiceId()) {
                            // release the slot acquired before the service was started
                            jacsServiceEngine.releaseSlot();
                        }
                        jacsServiceQueue.completeService(service);
                        if (exc != null) {
                            fail(service, exc);
                        } else {
                            success(service);
                        }
                    });
        }
    }

    private void success(JacsServiceData jacsServiceData) {
        logger.error("Processing successful {}:{}", jacsServiceData.getId(), jacsServiceData.getName());
        if (jacsServiceData.hasCompletedSuccessfully()) {
            // nothing to do
            logger.info("Service {} has already been marked as successful", jacsServiceData);
            return;
        }
        logger.error("Processing successful {}:{}", jacsServiceData.getId(), jacsServiceData.getName());
        if (jacsServiceData.hasCompletedUnsuccessfully()) {
            logger.warn("Attempted to overwrite failed state with success for {}", jacsServiceData);
            return;
        }
        jacsServiceData.updateState(JacsServiceState.SUCCESSFUL);
        jacsServiceData.addEvent(JacsServiceEventTypes.COMPLETED, "Completed successfully");
        updateServiceData(jacsServiceData);
    }

    private void fail(JacsServiceData jacsServiceData, Throwable exc) {
        if (jacsServiceData.hasCompletedUnsuccessfully()) {
            // nothing to do
            logger.info("Service {} has already been marked as failed", jacsServiceData);
            return;
        }
        logger.error("Processing error executing {}:{}", jacsServiceData.getId(), jacsServiceData.getName(), exc);
        if (jacsServiceData.hasCompletedSuccessfully()) {
            logger.warn("Service {} has failed after has already been markes as successfully completed", jacsServiceData);
        }
        jacsServiceData.updateState(JacsServiceState.ERROR);
        jacsServiceData.addEvent(JacsServiceEventTypes.FAILED, String.format("Failed: %s", exc.getMessage()));
        updateServiceData(jacsServiceData);
    }

    private void updateServiceData(JacsServiceData jacsServiceData) {
        jacsServiceDataPersistence.update(jacsServiceData);
    }

}

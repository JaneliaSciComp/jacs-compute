package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.asyncservice.JacsServiceEngine;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class JacsServiceDispatcher {

    private static final int DISPATCH_BATCH_SIZE = 20;

    private ServiceComputationFactory serviceComputationFactory;
    private JacsServiceQueue jacsServiceQueue;
    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private JacsServiceEngine jacsServiceEngine;
    private Logger logger;

    JacsServiceDispatcher() {
        // CDI required ctor
    }

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
                    logger.info("Abort service {} for now because there are not enough processing slots", queuedService);
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
                        try {
                            if (exc != null) {
                                fail(service, exc);
                            } else {
                                success(service);
                            }
                        } finally {
                            jacsServiceQueue.completeService(service);
                            if (!service.hasParentServiceId()) {
                                // release the slot acquired before the service was started
                                jacsServiceEngine.releaseSlot();
                            }
                        }
                    })
                    ;
        }
    }

    private void success(JacsServiceData jacsServiceData) {
        logger.info("Processing successful {}", jacsServiceData);
        if (jacsServiceData.hasCompletedSuccessfully()) {
            // nothing to do
            logger.debug("Service {} has already been marked as successful", jacsServiceData);
            return;
        }
        if (jacsServiceData.hasCompletedUnsuccessfully()) {
            logger.warn("Attempted to overwrite failed state with success for {}", jacsServiceData);
        }
        jacsServiceData.updateState(JacsServiceState.SUCCESSFUL);
        jacsServiceData.addEvent(JacsServiceEventTypes.COMPLETED, "Completed successfully");
        updateServiceData(jacsServiceData);
        if (!jacsServiceData.hasParentServiceId()) {
            archiveServiceData(jacsServiceData.getId());
        }
    }

    private void fail(JacsServiceData jacsServiceData, Throwable exc) {
        logger.error("Processing error executing {}", jacsServiceData, exc);
        if (jacsServiceData.hasCompletedUnsuccessfully()) {
            // nothing to do
            logger.debug("Service {} has already been marked as failed", jacsServiceData);
            return;
        }
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

    private void archiveServiceData(Number serviceId) {
        JacsServiceData jacsServiceDataHierarchy = jacsServiceDataPersistence.findServiceHierarchy(serviceId);
        jacsServiceDataPersistence.archiveHierarchy(jacsServiceDataHierarchy);
    }

}

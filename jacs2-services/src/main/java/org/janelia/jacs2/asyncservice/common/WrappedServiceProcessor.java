package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;

import java.util.Optional;

public class WrappedServiceProcessor<S extends ServiceProcessor<T>, T> implements ServiceProcessor<T> {

    private final ServiceComputationFactory computationFactory;
    private final JacsServiceDataPersistence jacsServiceDataPersistence;
    private final S wrappedProcessor;

    public WrappedServiceProcessor(ServiceComputationFactory computationFactory,
                                   JacsServiceDataPersistence jacsServiceDataPersistence,
                                   S wrappedProcessor) {
        this.computationFactory = computationFactory;
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
        this.wrappedProcessor = wrappedProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return wrappedProcessor.getMetadata();
    }

    @Override
    public JacsServiceData createServiceData(ServiceExecutionContext executionContext, ServiceArg... args) {
        return wrappedProcessor.createServiceData(executionContext, args);
    }

    @Override
    public ServiceResultHandler<T> getResultHandler() {
        return wrappedProcessor.getResultHandler();
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return wrappedProcessor.getErrorChecker();
    }

    @Override
    public ServiceComputation<JacsServiceResult<T>> process(JacsServiceData jacsServiceData) {
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenApply(sd -> submit(sd))
                .thenSuspendUntil(sd -> new ContinuationCond.Cond<>(sd, isDone(sd)))
                .thenApply(sdCond -> new JacsServiceResult<>(sdCond.getState(), wrappedProcessor.getResultHandler().getServiceDataResult(sdCond.getState())));
    }

    private JacsServiceData submit(JacsServiceData jacsServiceData) {
        JacsServiceData parentServiceData = jacsServiceDataPersistence.findServiceHierarchy(jacsServiceData.getParentServiceId());
        if (parentServiceData == null) {
            jacsServiceDataPersistence.saveHierarchy(jacsServiceData);
            return jacsServiceData;
        } else {
            Optional<JacsServiceData> existingInstance = parentServiceData.findSimilarDependency(jacsServiceData);
            if (existingInstance.isPresent()) {
                return existingInstance.get();
            } else {
                jacsServiceDataPersistence.saveHierarchy(jacsServiceData);
                jacsServiceDataPersistence.addServiceEvent(
                        jacsServiceData,
                        JacsServiceData.createServiceEvent(JacsServiceEventTypes.CREATE_CHILD_SERVICE, String.format("Created child service %s", jacsServiceData)));
                return jacsServiceData;
            }
        }
    }

    private boolean isDone(JacsServiceData jacsServiceData) {
        JacsServiceData refreshServiceData = jacsServiceDataPersistence.findById(jacsServiceData.getId());
        return refreshServiceData.hasCompleted();
    }

    public S getWrappedProcessor() {
        return wrappedProcessor;
    }
}

package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;

@MdcContext
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
        JacsServiceData submittedService = submit(jacsServiceData);
        PeriodicallyCheckableState<JacsServiceData> submittedServiceStateCheck = new PeriodicallyCheckableState<>(submittedService, ProcessorHelper.getSoftJobDurationLimitInSeconds(jacsServiceData.getResources()) / 100);
        return computationFactory.newCompletedComputation(submittedServiceStateCheck)
                .thenSuspendUntil((PeriodicallyCheckableState<JacsServiceData> sdState) -> new ContinuationCond.Cond<>(sdState, sdState.updateCheckTime() && isDone(sdState)))
                .thenApply((PeriodicallyCheckableState<JacsServiceData> sdState) -> getResult(sdState))
                ;
    }

    private JacsServiceData submit(JacsServiceData jacsServiceData) {
        return jacsServiceDataPersistence.createServiceIfNotFound(jacsServiceData);
    }

    private boolean isDone(PeriodicallyCheckableState<JacsServiceData> jacsServiceDataState) {
        JacsServiceData refreshServiceData = jacsServiceDataPersistence.findById(jacsServiceDataState.getState().getId());
        return refreshServiceData.hasCompleted();
    }

    private JacsServiceResult<T> getResult(PeriodicallyCheckableState<JacsServiceData> jacsServiceDataState) {
        JacsServiceData refreshServiceData = jacsServiceDataPersistence.findById(jacsServiceDataState.getState().getId());
        if (refreshServiceData.hasCompletedSuccessfully()) {
            return new JacsServiceResult<T>(refreshServiceData, wrappedProcessor.getResultHandler().getServiceDataResult(refreshServiceData));
        } else {
            throw new ComputationException(refreshServiceData, "Service " + refreshServiceData.toString() + " completed unsuccessfully");
        }
    }
}

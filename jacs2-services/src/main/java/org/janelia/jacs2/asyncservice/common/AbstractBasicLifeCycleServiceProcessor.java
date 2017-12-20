package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

/**
 * Abstract implementation of a service processor that "encodes" the life cycle of a computation. The class is parameterized
 * on a type dependent on the child services &lt;S&gt; and the result type of this service &lt;T&gt;.
 * @param <R> represents the result type
 * @param <S> is the type of an intermediate result that depends on the child services results. This doesn't necessarily have to contain the results of the
 *           child services but it can contain useful data related to the child service that could be used to derive the result of the
 *           current service.
 */
@MdcContext
public abstract class AbstractBasicLifeCycleServiceProcessor<R, S> extends AbstractServiceProcessor<R> {

    public AbstractBasicLifeCycleServiceProcessor(ServiceComputationFactory computationFactory,
                                                  JacsServiceDataPersistence jacsServiceDataPersistence,
                                                  String defaultWorkingDir,
                                                  Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
    }

    @Override
    public ServiceComputation<JacsServiceResult<R>> process(JacsServiceData jacsServiceData) {
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenApply(this::prepareProcessing)
                .thenApply(this::submitServiceDependencies)
                .thenSuspendUntil(new SuspendServiceContinuationCond<>(
                        (JacsServiceResult<S> pd) -> pd.getJacsServiceData(),
                        (JacsServiceResult<S> pd, JacsServiceData sd) -> new JacsServiceResult<>(sd, pd.getResult()),
                        jacsServiceDataPersistence,
                        logger).negate())
                .thenCompose(pd -> this.processing(pd))
                .thenSuspendUntil(pd -> new ContinuationCond.Cond<>(pd, this.isResultReady(pd)))
                .thenApply(pd -> this.updateServiceResult(pd))
                .thenApply(this::postProcessing)
                ;
    }

    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        JacsServiceData jacsServiceDataHierarchy = jacsServiceDataPersistence.findServiceHierarchy(jacsServiceData.getId());
        if (jacsServiceDataHierarchy == null) {
            jacsServiceDataHierarchy = jacsServiceData;
        }
        setOutputAndErrorPaths(jacsServiceDataHierarchy);
        return jacsServiceDataHierarchy;
    }

    /**
     * This method submits all sub-services and returns a
     * @param jacsServiceData current service data
     * @return a JacsServiceResult with a result of type U
     */
    protected JacsServiceResult<S> submitServiceDependencies(JacsServiceData jacsServiceData) {
        return new JacsServiceResult<>(jacsServiceData);
    }

    protected abstract ServiceComputation<JacsServiceResult<S>> processing(JacsServiceResult<S> depsResult);

    protected boolean isResultReady(JacsServiceResult<S> depsResults) {
        if (getResultHandler().isResultReady(depsResults)) {
            return true;
        }
        verifyAndFailIfTimeOut(depsResults.getJacsServiceData());
        return false;
    }

    protected JacsServiceResult<R> updateServiceResult(JacsServiceResult<S> depsResult) {
        R r = this.getResultHandler().collectResult(depsResult);
        JacsServiceData jacsServiceData = depsResult.getJacsServiceData();
        return updateServiceResult(jacsServiceData, r);
    }

    protected JacsServiceResult<R> postProcessing(JacsServiceResult<R> sr) {
        return sr;
    }
}

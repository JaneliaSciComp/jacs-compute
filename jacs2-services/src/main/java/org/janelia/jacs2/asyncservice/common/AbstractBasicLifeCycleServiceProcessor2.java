package org.janelia.jacs2.asyncservice.common;

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.asyncservice.common.resulthandlers.EmptyServiceResultHandler;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Abstract implementation of a service processor that "encodes" the life cycle of a computation. The class is parameterized
 * on a type dependent on the child services &lt;S&gt; and the result type of this service &lt;T&gt;.
 * @param <R> represents the result type
 */
@MdcContext
public abstract class AbstractBasicLifeCycleServiceProcessor2<R> extends AbstractServiceProcessor2<R> {

    @Override
    public ServiceComputation<JacsServiceResult<R>> process(JacsServiceData jacsServiceData) {
        try {
            return computationFactory.newCompletedComputation(jacsServiceData)
                    .thenApply(this::prepareProcessingUnchecked)
                    .thenApply(this::submitServiceDependenciesUnchecked)
                    .thenSuspendUntil(new WaitingForDependenciesContinuationCond<>(
                            (JacsServiceResult<Void> pd) -> pd.getJacsServiceData(),
                            (JacsServiceResult<Void> pd, JacsServiceData sd) -> new JacsServiceResult<>(sd, pd.getResult()),
                            jacsServiceDataPersistence,
                            logger).negate())
                    .thenCompose(pd -> this.processingUnchecked(pd))
                    .thenSuspendUntil(pd -> new ContinuationCond.Cond<>(pd, this.isResultReady(pd)))
                    .thenApply(pd -> this.updateServiceResult(pd))
                    .thenApply(this::postProcessingUnchecked)
                    .whenComplete((sr, t) -> {
                        JacsServiceData sd = jacsServiceDataPersistence.findById(jacsServiceData.getId());
                        doFinallyUnchecked(sd, t);
                    });
        }
        catch (Throwable t) {
            JacsServiceData sd = jacsServiceDataPersistence.findById(jacsServiceData.getId());
            doFinallyUnchecked(sd, t);
            throw t;
        }
    }

    @MdcContext
    protected JacsServiceData prepareProcessingUnchecked(JacsServiceData jacsServiceData) {
        try {
            return prepareProcessing(jacsServiceData);
        }
        catch (Exception e) {
            throw new UncheckedExecutionException(e);
        }
    }

    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) throws Exception {
        JacsServiceData jacsServiceDataHierarchy = jacsServiceDataPersistence.findServiceHierarchy(jacsServiceData.getId());
        if (jacsServiceDataHierarchy == null) {
            jacsServiceDataHierarchy = jacsServiceData;
        }
        return jacsServiceDataHierarchy;
    }

    protected JacsServiceResult<Void> submitServiceDependenciesUnchecked(JacsServiceData jacsServiceData) {
        try {
            return submitServiceDependencies(jacsServiceData);
        }
        catch (Exception e) {
            throw new UncheckedExecutionException(e);
        }
    }

    /**
     * This method submits all sub-services and returns a
     * @param jacsServiceData current service data
     * @return a JacsServiceResult with a result of type U
     */
    protected JacsServiceResult<Void> submitServiceDependencies(JacsServiceData jacsServiceData) throws Exception {
        return new JacsServiceResult<>(jacsServiceData);
    }

    protected JacsServiceData submitDependencyIfNotFound(JacsServiceData dependency) {
        return jacsServiceDataPersistence.createServiceIfNotFound(dependency);
    }

    @MdcContext
    protected boolean areAllDependenciesDone(JacsServiceData jacsServiceData) {
        return new ServiceDependenciesCompletedContinuationCond(
                dependenciesGetterFunc(),
                jacsServiceDataPersistence,
                logger).checkCond(jacsServiceData).isCondValue();
    }

    protected Function<JacsServiceData, Stream<JacsServiceData>> dependenciesGetterFunc() {
        return (JacsServiceData serviceData) -> jacsServiceDataPersistence.findServiceDependencies(serviceData).stream();
    }

    @MdcContext
    protected ServiceComputation<JacsServiceResult<Void>> processingUnchecked(JacsServiceResult<Void> depsResult) {
        try {
            return processing(depsResult);
        }
        catch (Exception e) {
            throw new UncheckedExecutionException(e);
        }
    }

    protected ServiceComputation<JacsServiceResult<Void>> processing(JacsServiceResult<Void> depsResult) throws Exception {
        return computationFactory.newCompletedComputation(depsResult);
    }

    @MdcContext
    protected boolean isResultReady(JacsServiceResult<Void> depsResults) {
        if (getResultHandler().isResultReady(depsResults)) {
            return true;
        }
        verifyAndFailIfTimeOut(depsResults.getJacsServiceData());
        return false;
    }

    void verifyAndFailIfTimeOut(JacsServiceData jacsServiceData) {
        long timeSinceStart = System.currentTimeMillis() - jacsServiceData.getProcessStartTime().getTime();
        if (jacsServiceData.timeout() > 0 && timeSinceStart > jacsServiceData.timeout()) {
            jacsServiceDataPersistence.updateServiceState(
                    jacsServiceData,
                    JacsServiceState.TIMEOUT,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.TIMEOUT, String.format("Service timed out after %s ms", timeSinceStart)));
            logger.warn("Service {} timed out after {}ms", jacsServiceData, timeSinceStart);
            throw new ComputationException(jacsServiceData, "Service " + jacsServiceData.getId() + " timed out");
        }
    }

    @MdcContext
    protected JacsServiceResult<R> updateServiceResult(JacsServiceResult<Void> depsResult) {
        JacsServiceData jacsServiceData = depsResult.getJacsServiceData();

        R r;
        ServiceResultHandler<R> resultHandler = this.getResultHandler();
        if (resultHandler instanceof EmptyServiceResultHandler) {
            try {
                r = execute(jacsServiceData);
                jacsServiceData.setSerializableResult(r);
                jacsServiceDataPersistence.updateServiceResult(jacsServiceData);
            }
            catch (Exception e) {
                throw new UncheckedExecutionException(e);
            }
        }
        else {
            r = this.getResultHandler().collectResult(depsResult);
        }

        return updateServiceResult(jacsServiceData, r);
    }

    protected R execute(JacsServiceData sd) throws Exception {
        return null;
    }

    @MdcContext
    private JacsServiceResult<R> postProcessingUnchecked(JacsServiceResult<R> sr) {
        try {
            return postProcessing(sr);
        }
        catch (Exception e) {
            throw new UncheckedExecutionException(e);
        }
    }

    protected JacsServiceResult<R> postProcessing(JacsServiceResult<R> sr) throws Exception {
        return sr;
    }

    private void doFinallyUnchecked(JacsServiceData jacsServiceData, Throwable throwable) {
        try {
            doFinally(jacsServiceData, throwable);
        }
        catch (Exception e) {
            throw new UncheckedExecutionException(e);
        }
    }

    /**
     * This method executes right before the service is complete, whether or not it encountered an
     * exception. The JacsServiceData is always guaranteed to be not null, and up-to-date.
     * @param jacsServiceData
     * @param throwable
     * @throws Exception
     */
    protected void doFinally(JacsServiceData jacsServiceData, Throwable throwable) throws Exception {
    }
}

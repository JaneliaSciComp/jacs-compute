package org.janelia.jacs2.asyncservice.common;

import com.google.common.util.concurrent.UncheckedExecutionException;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.model.service.JacsServiceData;

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
                    .thenCompose(this::processingUnchecked)
                    .thenApply(this::createResultUnchecked)
                    .whenComplete(this::doFinallyUnchecked);
        }
        catch (Throwable t) {
            JacsServiceData sd = jacsServiceDataPersistence.findById(jacsServiceData.getId());
            doFinallyUnchecked(new JacsServiceResult<>(sd), t);
            throw t;
        }
    }

    @MdcContext
    protected JacsServiceData prepareProcessingUnchecked(JacsServiceData sd) {
        currentService.setJacsServiceData(sd);
        try {
            return prepareProcessing(sd);
        }
        catch (Exception e) {
            throw new UncheckedExecutionException(e);
        }
    }

    protected JacsServiceData prepareProcessing(JacsServiceData sd) throws Exception {
        return sd;
//        JacsServiceData jacsServiceDataHierarchy = jacsServiceDataPersistence.findServiceHierarchy(sd.getId());
//        if (jacsServiceDataHierarchy == null) {
//            jacsServiceDataHierarchy = sd;
//        }
//        return jacsServiceDataHierarchy;
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
    protected ServiceComputation<JacsServiceData> processingUnchecked(JacsServiceData sd) {
        currentService.setJacsServiceData(sd);
        try {
            return processing(sd);
        }
        catch (Exception e) {
            throw new UncheckedExecutionException(e);
        }
    }

    protected ServiceComputation<JacsServiceData> processing(JacsServiceData sd) throws Exception {
        execute(sd);
        return computationFactory.newCompletedComputation(sd);
    }

    protected void execute(JacsServiceData sd) throws Exception {
    }

    @MdcContext
    private JacsServiceResult<R> createResultUnchecked(JacsServiceData sd) {
        currentService.setJacsServiceData(sd);
        try {
            R r = createResult();
            if (r == null) {
                return new JacsServiceResult<R>(sd);
            }
            sd.setSerializableResult(r);
            jacsServiceDataPersistence.updateServiceResult(sd);
            return new JacsServiceResult<R>(sd, r);
        }
        catch (Exception e) {
            throw new UncheckedExecutionException(e);
        }
    }

    protected R createResult() throws Exception {
        return null;
    }

    private JacsServiceResult<R> doFinallyUnchecked(JacsServiceResult<R> sr, Throwable throwable) {
        JacsServiceData sd = sr.getJacsServiceData();
        currentService.setJacsServiceData(sd);
        try {
            doFinally(sd, throwable);
        }
        catch (Exception e) {
            throw new UncheckedExecutionException(e);
        }
        return sr;
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

package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Abstract implementation of a service processor that "encodes" the life cycle of a computation. The class is parameterized
 * on a type dependent on the child services &lt;S&gt; and the result type of this service &lt;T&gt;.
 * @param <S> a specific type that defines the child services results. This doesn't necessarily have to contain the results of the
 *           child services but it can contain useful data related to the child service that could be used to derive the result of the
 *           current service
 * @param <T> represents the result type
 */
@MdcContext
public abstract class AbstractBasicLifeCycleServiceProcessor<S, T> extends AbstractServiceProcessor<T> {

    public AbstractBasicLifeCycleServiceProcessor(ServiceComputationFactory computationFactory,
                                                  JacsServiceDataPersistence jacsServiceDataPersistence,
                                                  String defaultWorkingDir,
                                                  Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
    }

    @Override
    public ServiceComputation<JacsServiceResult<T>> process(JacsServiceData jacsServiceData) {
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenApply(this::prepareProcessing)
                .thenApply(this::submitServiceDependencies)
                .thenSuspendUntil(pd -> new ContinuationCond.Cond<>(pd, !suspendUntilAllDependenciesComplete(pd.getJacsServiceData())))
                .thenCompose(pdCond -> this.processing(pdCond.getState()))
                .thenSuspendUntil(pd -> new ContinuationCond.Cond<>(pd, this.isResultReady(pd)))
                .thenApply(pdCond -> this.updateServiceResult(pdCond.getState()))
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

    /**
     * Suspend the service until the given service is done or until all dependencies are done
     * @param jacsServiceData service data
     * @return true if the service has been suspended
     */
    protected boolean suspendUntilAllDependenciesComplete(JacsServiceData jacsServiceData) {
        if (jacsServiceData.hasCompleted()) {
            return false;
        }
        return areAllDependenciesDoneFunc()
                .andThen(pd -> {
                    JacsServiceData sd = pd.getJacsServiceData();
                    boolean depsCompleted = pd.getResult();
                    if (depsCompleted) {
                        resumeSuspendedService(sd);
                        return false;
                    } else {
                        suspendService(sd);
                        return true;
                    }
                })
                .apply(jacsServiceData);
    }

    protected boolean areAllDependenciesDone(JacsServiceData jacsServiceData) {
        return areAllDependenciesDoneFunc().apply(jacsServiceData).getResult();
    }

    /**
     * This function is related to the state monad bind operator in which a state is a function from a
     * state to a (state, value) pair.
     * @return a function from a servicedata to a service data. The function's application updates the service data.
     */
    protected Function<JacsServiceData, JacsServiceResult<Boolean>> areAllDependenciesDoneFunc() {
        return sdp -> {
            List<JacsServiceData> running = new ArrayList<>();
            List<JacsServiceData> failed = new ArrayList<>();
            if (!sdp.hasId()) {
                return new JacsServiceResult<>(sdp, true);
            }
            // check if the children and the immediate dependencies are done
            List<JacsServiceData> childServices = jacsServiceDataPersistence.findChildServices(sdp.getId());
            List<JacsServiceData> dependentServices = jacsServiceDataPersistence.findByIds(sdp.getDependenciesIds());
            Stream.concat(
                    childServices.stream(),
                    dependentServices.stream())
                    .forEach(sd -> {
                        if (!sd.hasCompleted()) {
                            running.add(sd);
                        } else if (sd.hasCompletedUnsuccessfully()) {
                            failed.add(sd);
                        }
                    });
            if (CollectionUtils.isNotEmpty(failed)) {
                jacsServiceDataPersistence.updateServiceState(
                        sdp,
                        JacsServiceState.CANCELED,
                        Optional.of(JacsServiceData.createServiceEvent(
                                JacsServiceEventTypes.CANCELED,
                                String.format("Canceled because one or more service dependencies finished unsuccessfully: %s", failed))));
                logger.warn("Service {} canceled because of {}", sdp, failed);
                throw new ComputationException(sdp, "Service " + sdp.getId() + " canceled");
            }
            if (CollectionUtils.isEmpty(running)) {
                return new JacsServiceResult<>(sdp, true);
            }
            verifyAndFailIfTimeOut(sdp);
            return new JacsServiceResult<>(sdp, false);
        };
    }

    protected void resumeSuspendedService(JacsServiceData jacsServiceData) {
        jacsServiceDataPersistence.updateServiceState(jacsServiceData, JacsServiceState.RUNNING, Optional.empty());
    }

    protected void suspendService(JacsServiceData jacsServiceData) {
        if (!jacsServiceData.hasBeenSuspended()) {
            // if the service has not completed yet and it's not already suspended - update the state to suspended
            jacsServiceDataPersistence.updateServiceState(jacsServiceData, JacsServiceState.SUSPENDED, Optional.empty());
        }
    }

    protected void verifyAndFailIfTimeOut(JacsServiceData jacsServiceData) {
        long timeSinceStart = System.currentTimeMillis() - jacsServiceData.getProcessStartTime().getTime();
        if (jacsServiceData.timeout() > 0 && timeSinceStart > jacsServiceData.timeout()) {
            jacsServiceDataPersistence.updateServiceState(
                    jacsServiceData,
                    JacsServiceState.TIMEOUT,
                    Optional.of(JacsServiceData.createServiceEvent(JacsServiceEventTypes.TIMEOUT, String.format("Service timed out after %s ms", timeSinceStart))));
            logger.warn("Service {} timed out after {}ms", jacsServiceData, timeSinceStart);
            throw new ComputationException(jacsServiceData, "Service " + jacsServiceData.getId() + " timed out");
        }
    }

    protected abstract ServiceComputation<JacsServiceResult<S>> processing(JacsServiceResult<S> depsResult);

    protected boolean isResultReady(JacsServiceResult<S> depsResults) {
        if (getResultHandler().isResultReady(depsResults)) {
            return true;
        }
        verifyAndFailIfTimeOut(depsResults.getJacsServiceData());
        return false;
    }

    protected JacsServiceResult<T> updateServiceResult(JacsServiceResult<S> depsResult) {
        T r = this.getResultHandler().collectResult(depsResult);
        JacsServiceData jacsServiceData = depsResult.getJacsServiceData();
        this.getResultHandler().updateServiceDataResult(jacsServiceData, r);
        jacsServiceDataPersistence.updateServiceResult(jacsServiceData);
        return new JacsServiceResult<>(jacsServiceData, r);
    }

    protected JacsServiceResult<T> postProcessing(JacsServiceResult<T> sr) {
        return sr;
    }

    protected JacsServiceData submitDependencyIfNotPresent(JacsServiceData jacsServiceData, JacsServiceData dependency) {
        JacsServiceData refreshedServiceData = jacsServiceDataPersistence.findServiceHierarchy(jacsServiceData.getId());
        Optional<JacsServiceData> existingDependency = refreshedServiceData.findSimilarDependency(dependency);
        if (existingDependency.isPresent()) {
            return existingDependency.get();
        } else {
            jacsServiceDataPersistence.saveHierarchy(dependency);
            jacsServiceDataPersistence.addServiceEvent(
                    jacsServiceData,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.CREATE_CHILD_SERVICE, String.format("Created child service %s", dependency)));
            return dependency;
        }
    }

}

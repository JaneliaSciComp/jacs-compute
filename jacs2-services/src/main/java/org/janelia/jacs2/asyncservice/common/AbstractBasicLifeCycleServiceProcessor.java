package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.jacs2.asyncservice.utils.DataHolder;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Abstract implementation of a service processor that "encodes" the life cycle of a computation. The class is parameterized
 * on a type dependent on the child services &lt;S&gt; and the result type of this service &lt;T&gt;.
 * @param <S> a specific type that defines the child services results. This doesn't necessarily have to contain the results of the
 *           child services but it can contain useful data related to the child service that could be used to derive the result of the
 *           current service
 * @param <T> represents the result type
 */
public abstract class AbstractBasicLifeCycleServiceProcessor<S, T> extends AbstractServiceProcessor<T> {

    public AbstractBasicLifeCycleServiceProcessor(ServiceComputationFactory computationFactory,
                                                  JacsServiceDataPersistence jacsServiceDataPersistence,
                                                  String defaultWorkingDir,
                                                  Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
    }

    @Override
    public ServiceComputation<T> process(JacsServiceData jacsServiceData) {
        DataHolder<JacsServiceResult<S>> processDataHolder = new DataHolder<>(); // this will enclose the service data with the changes made by the prepareProcessing method in case there are any
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenApply(sd -> {
                    JacsServiceData preparedJacsServiceData = this.prepareProcessing(sd);
                    processDataHolder.setData(new JacsServiceResult<>(preparedJacsServiceData));
                    processDataHolder.setData(this.submitServiceDependencies(preparedJacsServiceData));
                    return processDataHolder.getData();
                })
                .thenSuspendUntil(() -> !suspendUntilAllDependenciesComplete(processDataHolder.getData().getJacsServiceData())) // suspend until all dependencies complete
                .thenCompose(this::processing)
                .thenSuspendUntil(() -> this.isResultReady(processDataHolder.getData())) // wait until the result becomes available
                .thenApply(pd -> {
                    T r = this.getResultHandler().collectResult(pd);
                    this.getResultHandler().updateServiceDataResult(pd.getJacsServiceData(), r);
                    updateServiceData(pd.getJacsServiceData());
                    return new JacsServiceResult<>(pd.getJacsServiceData(), r);
                })
                .thenApply(this::postProcessing)
                ;
    }

    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        JacsServiceData jacsServiceDataHierarchy = jacsServiceDataPersistence.findServiceHierarchy(jacsServiceData.getId());
        if (jacsServiceDataHierarchy == null) {
            jacsServiceDataHierarchy = jacsServiceData;
        }
        setOutputPath(jacsServiceDataHierarchy);
        setErrorPath(jacsServiceDataHierarchy);
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

    protected boolean isResultReady(JacsServiceResult<S> depsResults) {
        if (getResultHandler().isResultReady(depsResults)) {
            return true;
        }
        verifyTimeOut(depsResults.getJacsServiceData());
        return false;
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
                    JacsServiceData sd = pd.getLeft();
                    boolean depsCompleted = pd.getRight();
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
        return areAllDependenciesDoneFunc().apply(jacsServiceData).getRight();
    }

    private Function<JacsServiceData, Pair<JacsServiceData, Boolean>> areAllDependenciesDoneFunc() {
        return sdp -> {
            List<JacsServiceData> running = new ArrayList<>();
            List<JacsServiceData> failed = new ArrayList<>();
            JacsServiceData jacsServiceData = jacsServiceDataPersistence.findServiceHierarchy(sdp.getId());
            if (jacsServiceData == null) {
                return new ImmutablePair<>(sdp, true);
            }
            jacsServiceData.serviceHierarchyStream()
                    .filter(sd -> !sd.getId().equals(jacsServiceData.getId()))
                    .forEach(sd -> {
                        if (!sd.hasCompleted()) {
                            running.add(sd);
                        } else if (sd.hasCompletedUnsuccessfully()) {
                            failed.add(sd);
                        }
                    });
            if (CollectionUtils.isNotEmpty(failed)) {
                jacsServiceData.updateState(JacsServiceState.CANCELED);
                jacsServiceData.addEvent(JacsServiceEventTypes.CANCELED,
                        String.format("Canceled because one or more service dependencies finished unsuccessfully: %s", failed));
                updateServiceData(jacsServiceData);
                logger.warn("Service {} canceled because of {}", jacsServiceData, failed);
                throw new ComputationException(jacsServiceData, "Service " + jacsServiceData.getId() + " canceled");
            }
            if (CollectionUtils.isEmpty(running)) {
                return new ImmutablePair<>(jacsServiceData, true);
            }
            verifyTimeOut(jacsServiceData);
            return new ImmutablePair<>(jacsServiceData, false);
        };
    }

    private void resumeSuspendedService(JacsServiceData jacsServiceData) {
        updateState(jacsServiceData, JacsServiceState.RUNNING);
    }

    private void suspendService(JacsServiceData jacsServiceData) {
        if (!jacsServiceData.hasBeenSuspended()) {
            // if the service has not completed yet and it's not already suspended - update the state to suspended
            updateState(jacsServiceData, JacsServiceState.SUSPENDED);
        }
    }

    private void updateState(JacsServiceData jacsServiceData, JacsServiceState state) {
        try {
            jacsServiceData.updateState(state);
        } catch (Exception e) {
            logger.error("Update state error for {}", jacsServiceData, e);
            throw new ComputationException(jacsServiceData, e);
        } finally {
            updateServiceData(jacsServiceData);
        }
    }

    protected void  verifyTimeOut(JacsServiceData jacsServiceData) {
        long timeSinceStart = System.currentTimeMillis() - jacsServiceData.getProcessStartTime().getTime();
        if (jacsServiceData.timeout() > 0 && timeSinceStart > jacsServiceData.timeout()) {
            jacsServiceData.updateState(JacsServiceState.TIMEOUT);
            jacsServiceData.addEvent(JacsServiceEventTypes.TIMEOUT, String.format("Service timed out after %s ms", timeSinceStart));
            jacsServiceDataPersistence.update(jacsServiceData);
            logger.warn("Service {} timed out after {}ms", jacsServiceData, timeSinceStart);
            throw new ComputationException(jacsServiceData, "Service " + jacsServiceData.getId() + " timed out");
        }
    }

    protected abstract ServiceComputation<JacsServiceResult<S>> processing(JacsServiceResult<S> depsResult);

    protected T postProcessing(JacsServiceResult<T> sr) {
        return sr.getResult();
    }

    protected JacsServiceData submitDependencyIfNotPresent(JacsServiceData jacsServiceData, JacsServiceData dependency) {
        Optional<JacsServiceData> existingDependency = jacsServiceData.findSimilarDependency(dependency);
        if (existingDependency.isPresent()) {
            return existingDependency.get();
        } else {
            jacsServiceDataPersistence.saveHierarchy(dependency);
            return dependency;
        }
    }

}

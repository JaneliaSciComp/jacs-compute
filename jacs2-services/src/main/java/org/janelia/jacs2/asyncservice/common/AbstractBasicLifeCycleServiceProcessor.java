package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.asyncservice.utils.DataHolder;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
        if (areAllDependenciesDone(jacsServiceData)) {
            if (jacsServiceData.hasBeenSuspended()) {
                jacsServiceData.setState(JacsServiceState.RUNNING);
                updateServiceData(jacsServiceData);
            }
            return false;
        } else {
            if (!jacsServiceData.hasBeenSuspended()) {
                // if the service has not completed yet and it's not already suspended - update the state to suspended
                jacsServiceData.setState(JacsServiceState.SUSPENDED);
                updateServiceData(jacsServiceData);
            }
            return true;
        }
    }

    protected boolean areAllDependenciesDone(JacsServiceData jacsServiceData) {
        List<JacsServiceData> running = new ArrayList<>();
        List<JacsServiceData> failed = new ArrayList<>();
        JacsServiceData jacsServiceDataHierarchy = jacsServiceDataPersistence.findServiceHierarchy(jacsServiceData.getId());
        if (jacsServiceDataHierarchy == null) {
            return true;
        }
        jacsServiceDataHierarchy.serviceHierarchyStream()
                .filter(sd -> !sd.getId().equals(jacsServiceData.getId()))
                .forEach(sd -> {
                    if (!sd.hasCompleted()) {
                        running.add(sd);
                    } else if (sd.hasCompletedUnsuccessfully()) {
                        failed.add(sd);
                    }
                });
        if (CollectionUtils.isNotEmpty(failed)) {
            jacsServiceData.setState(JacsServiceState.CANCELED);
            jacsServiceData.addEvent(JacsServiceEventTypes.CANCELED,
                    String.format("Canceled because one or more service dependencies finished unsuccessfully: %s", failed));
            updateServiceData(jacsServiceData);
            logger.warn("Service {} canceled because of {}", jacsServiceData, failed);
            throw new ComputationException(jacsServiceData, "Service " + jacsServiceData.getId() + " canceled");
        }
        if (CollectionUtils.isEmpty(running)) {
            return true;
        }
        verifyTimeOut(jacsServiceData);
        return false;
    }

    protected void  verifyTimeOut(JacsServiceData jacsServiceData) {
        long timeSinceStart = System.currentTimeMillis() - jacsServiceData.getProcessStartTime().getTime();
        if (jacsServiceData.timeout() > 0 && timeSinceStart > jacsServiceData.timeout()) {
            jacsServiceData.setState(JacsServiceState.TIMEOUT);
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

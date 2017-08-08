package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * SuspendServiceContinuationCond implements a Continuation that may suspend a service if its dependencies are not complete.
 */
public class SuspendServiceContinuationCond<S> implements ContinuationCond<S> {

    private final ContinuationCond<JacsServiceData> dependenciesCompletedCont;
    private final Function<S, JacsServiceData> stateToServiceDataMapper;
    private final BiFunction<S, JacsServiceData, S> serviceDataToStateMapper;
    private final JacsServiceDataPersistence jacsServiceDataPersistence;
    private final Logger logger;

    public SuspendServiceContinuationCond(Function<S, JacsServiceData> stateToServiceDataMapper,
                                          BiFunction<S, JacsServiceData, S> serviceDataToStateMapper,
                                          JacsServiceDataPersistence jacsServiceDataPersistence,
                                          Logger logger) {
        this(new ServiceDependenciesCompletedContinuationCond(jacsServiceDataPersistence, logger),
                stateToServiceDataMapper,
                serviceDataToStateMapper,
                jacsServiceDataPersistence,
                logger
        );
    }

    public SuspendServiceContinuationCond(ContinuationCond<JacsServiceData> dependenciesCompletedCont,
                                          Function<S, JacsServiceData> stateToServiceDataMapper,
                                          BiFunction<S, JacsServiceData, S> serviceDataToStateMapper,
                                          JacsServiceDataPersistence jacsServiceDataPersistence,
                                          Logger logger) {
        this.dependenciesCompletedCont = dependenciesCompletedCont;
        this.stateToServiceDataMapper = stateToServiceDataMapper;
        this.serviceDataToStateMapper = serviceDataToStateMapper;
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
        this.logger = logger;
    }

    /**
     * @param state
     * @return a condition that evaluates to true if the service argument needs to be suspended.
     */
    @Override
    public Cond<S> checkCond(S state) {
        JacsServiceData updatedServiceData = refreshServiceData(stateToServiceDataMapper.apply(state));
        if (updatedServiceData.hasCompleted()) {
            return new Cond<>(serviceDataToStateMapper.apply(state, updatedServiceData), false);
        }
        Cond<JacsServiceData> dependenciesCompletedCond = dependenciesCompletedCont.checkCond(updatedServiceData);
        return new Cond<>(
                serviceDataToStateMapper.apply(
                        state,
                        dependenciesCompletedCond.isCondValue()
                                ? this.resumeService(dependenciesCompletedCond.getState())
                                : this.suspendService(dependenciesCompletedCond.getState())),
                dependenciesCompletedCond.isNotCondValue()
        );
    }

    private JacsServiceData refreshServiceData(JacsServiceData jacsServiceData) {
        return jacsServiceData.hasId() ? jacsServiceDataPersistence.findById(jacsServiceData.getId()) : jacsServiceData;
    }

    private JacsServiceData resumeService(JacsServiceData jacsServiceData) {
        if (jacsServiceData.getState() != JacsServiceState.RUNNING) {
            jacsServiceDataPersistence.updateServiceState(jacsServiceData, JacsServiceState.RUNNING, Optional.empty());
        }
        return jacsServiceData;
    }

    private JacsServiceData suspendService(JacsServiceData jacsServiceData) {
        if (!jacsServiceData.hasBeenSuspended()) {
            // if the service has not completed yet and it's not already suspended - update the state to suspended
            jacsServiceDataPersistence.updateServiceState(jacsServiceData, JacsServiceState.SUSPENDED, Optional.empty());
        }
        return jacsServiceData;
    }

}

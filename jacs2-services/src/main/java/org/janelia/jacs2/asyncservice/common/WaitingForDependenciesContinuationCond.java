package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEvent;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * WaitingForDependenciesContinuationCond implements a Continuation that may suspend a service if its dependencies are not complete.
 */
public class WaitingForDependenciesContinuationCond<S> implements ContinuationCond<S> {

    private final ContinuationCond<JacsServiceData> dependenciesCompletedCont;
    private final Function<S, JacsServiceData> stateToServiceDataMapper;
    private final BiFunction<S, JacsServiceData, S> serviceDataToStateMapper;
    private final JacsServiceDataPersistence jacsServiceDataPersistence;
    private final Logger logger;

    public WaitingForDependenciesContinuationCond(Function<S, JacsServiceData> stateToServiceDataMapper,
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

    public WaitingForDependenciesContinuationCond(ContinuationCond<JacsServiceData> dependenciesCompletedCont,
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
            jacsServiceDataPersistence.updateServiceState(jacsServiceData, JacsServiceState.RUNNING,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.RUN, "Service resumed"));
        }
        return jacsServiceData;
    }

    private JacsServiceData suspendService(JacsServiceData jacsServiceData) {
        if (jacsServiceData.hasNotBeenWaitingForDependencies()) {
            // if the service has not completed yet and it's not already suspended - update the state to suspended
            jacsServiceDataPersistence.updateServiceState(jacsServiceData, JacsServiceState.WAITING_FOR_DEPENDENCIES,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.SUSPEND, "Service suspended"));
        }
        return jacsServiceData;
    }

}

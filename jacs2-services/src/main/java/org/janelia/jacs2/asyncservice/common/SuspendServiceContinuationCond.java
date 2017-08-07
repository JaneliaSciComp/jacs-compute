package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * SuspendServiceContinuationCond implements a Continuation that may suspend a service if its dependencies are not complete.
 */
public class SuspendServiceContinuationCond implements ContinuationCond<JacsServiceData> {

    private final JacsServiceDataPersistence jacsServiceDataPersistence;
    private final Logger logger;

    public SuspendServiceContinuationCond(JacsServiceDataPersistence jacsServiceDataPersistence, Logger logger) {
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
        this.logger = logger;
    }

    /**
     * @param serviceData
     * @return a condition that evaluates to true if the service argument needs to be suspended.
     */
    @Override
    public Cond<JacsServiceData> checkCond(JacsServiceData serviceData) {
        JacsServiceData updatedServiceData = refreshServiceData(serviceData);

        if (updatedServiceData.hasCompleted()) {
            return new Cond<>(updatedServiceData, false);
        }

        List<JacsServiceData> runningDependencies = new ArrayList<>();
        List<JacsServiceData> failedDependencies = new ArrayList<>();
        // check if the children and the immediate dependencies are done
        List<JacsServiceData> serviceDependencies = jacsServiceDataPersistence.findServiceDependencies(updatedServiceData);
        verifyAndFailIfAnyDependencyFailed(updatedServiceData, failedDependencies);
        serviceDependencies.stream()
                .forEach(sd -> {
                    if (!sd.hasCompleted()) {
                        runningDependencies.add(sd);
                    } else if (sd.hasCompletedUnsuccessfully()) {
                        failedDependencies.add(sd);
                    }
                });
        if (CollectionUtils.isEmpty(runningDependencies)) {
            return new Cond<>(resumeService(updatedServiceData), false);
        }
        verifyAndFailIfTimeOut(updatedServiceData);
        return new Cond<>(suspendService(updatedServiceData), true);
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

    private void verifyAndFailIfAnyDependencyFailed(JacsServiceData jacsServiceData, List<JacsServiceData> failedDependencies) {
        if (CollectionUtils.isNotEmpty(failedDependencies)) {
            jacsServiceDataPersistence.updateServiceState(
                    jacsServiceData,
                    JacsServiceState.CANCELED,
                    Optional.of(JacsServiceData.createServiceEvent(
                            JacsServiceEventTypes.CANCELED,
                            String.format("Canceled because one or more service dependencies finished unsuccessfully: %s", failedDependencies))));
            logger.warn("Service {} canceled because of {}", jacsServiceData, failedDependencies);
            throw new ComputationException(jacsServiceData, "Service " + jacsServiceData.getEntityRefId() + " canceled");
        }
    }

    private void verifyAndFailIfTimeOut(JacsServiceData jacsServiceData) {
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

}

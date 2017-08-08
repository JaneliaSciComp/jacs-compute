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
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * SuspendServiceContinuationCond implements a Continuation that may suspend a service if its dependencies are not complete.
 */
public class ServiceDependenciesCompletedContinuationCond implements ContinuationCond<JacsServiceData> {

    private final Function<JacsServiceData, Stream<JacsServiceData>> dependenciesGetter;
    private final JacsServiceDataPersistence jacsServiceDataPersistence;
    private final Logger logger;

    public ServiceDependenciesCompletedContinuationCond(JacsServiceDataPersistence jacsServiceDataPersistence,
                                                        Logger logger) {
        this((JacsServiceData serviceData) -> jacsServiceDataPersistence.findServiceDependencies(serviceData).stream(),
                jacsServiceDataPersistence,
                logger
        );
    }

    public ServiceDependenciesCompletedContinuationCond(Function<JacsServiceData, Stream<JacsServiceData>> dependenciesGetter,
                                                        JacsServiceDataPersistence jacsServiceDataPersistence,
                                                        Logger logger) {
        this.dependenciesGetter = dependenciesGetter;
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
        this.logger = logger;
    }

    @Override
    public Cond<JacsServiceData> checkCond(JacsServiceData state) {
        return areAllDependenciesDone(state);
    }

    private Cond<JacsServiceData> areAllDependenciesDone(JacsServiceData jacsServiceData) {
        // check if all dependencies are done
        List<JacsServiceData> runningDependencies = new ArrayList<>();
        List<JacsServiceData> failedDependencies = new ArrayList<>();
        dependenciesGetter.apply(jacsServiceData)
                .forEach(sd -> {
                    if (!sd.hasCompleted()) {
                        runningDependencies.add(sd);
                    } else if (sd.hasCompletedUnsuccessfully()) {
                        failedDependencies.add(sd);
                    }
                });
        verifyAndFailIfAnyDependencyFailed(jacsServiceData, failedDependencies);
        if (CollectionUtils.isEmpty(runningDependencies)) {
            return new Cond<>(jacsServiceData, true);
        }
        verifyAndFailIfTimeOut(jacsServiceData);
        return new Cond<>(jacsServiceData, false);
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

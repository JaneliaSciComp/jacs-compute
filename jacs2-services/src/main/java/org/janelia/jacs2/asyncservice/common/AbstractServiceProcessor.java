package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.resulthandlers.EmptyServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceDataBuilder;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public abstract class AbstractServiceProcessor<T> implements ServiceProcessor<T> {

    protected final ServiceComputationFactory computationFactory;
    protected final JacsServiceDataPersistence jacsServiceDataPersistence;
    protected final String defaultWorkingDir;
    protected final Logger logger;

    public AbstractServiceProcessor(ServiceComputationFactory computationFactory,
                                    JacsServiceDataPersistence jacsServiceDataPersistence,
                                    String defaultWorkingDir,
                                    Logger logger) {
        this.computationFactory = computationFactory;
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
        this.defaultWorkingDir = defaultWorkingDir;
        this.logger = logger;
    }

    @Override
    public JacsServiceData createServiceData(ServiceExecutionContext executionContext, ServiceArg... args) {
        ServiceMetaData smd = getMetadata();
        JacsServiceDataBuilder jacsServiceDataBuilder =
                new JacsServiceDataBuilder(executionContext.getParentServiceData())
                        .setProcessingLocation(executionContext.getProcessingLocation())
                        .setDescription(executionContext.getDescription());
        if (executionContext.getServiceName() != null) {
            jacsServiceDataBuilder.setName(executionContext.getServiceName());
        } else {
            jacsServiceDataBuilder.setName(smd.getServiceName());
        }
        if (executionContext.getWorkingDirectory() != null) {
            jacsServiceDataBuilder.setWorkspace(executionContext.getWorkingDirectory());
        } else if (executionContext.getParentServiceData() != null) {
            Path parentWorkingDir = getWorkingDirectory(executionContext.getParentServiceData());
            jacsServiceDataBuilder.setWorkspace(Objects.toString(parentWorkingDir, null));
        }
        jacsServiceDataBuilder.addArg(Stream.of(args).flatMap(arg -> Stream.of(arg.toStringArray())).toArray(String[]::new));
        if (executionContext.getServiceState() != null) {
            jacsServiceDataBuilder.setState(executionContext.getServiceState());
        }
        if (StringUtils.isNotBlank(executionContext.getOutputPath())) {
            jacsServiceDataBuilder.setOutputPath(executionContext.getOutputPath());
        }
        if (StringUtils.isNotBlank(executionContext.getErrorPath())) {
            jacsServiceDataBuilder.setErrorPath(executionContext.getErrorPath());
        }
        jacsServiceDataBuilder.copyResourcesFrom(executionContext.getResources());
        executionContext.getWaitFor().forEach(jacsServiceDataBuilder::addDependency);
        executionContext.getWaitForIds().forEach(jacsServiceDataBuilder::addDependencyId);
        if (executionContext.getParentServiceData() != null) {
            executionContext.getParentServiceData().getDependenciesIds().forEach(jacsServiceDataBuilder::addDependencyId);
        }
        return jacsServiceDataBuilder.build();
    }

    @Override
    public ServiceResultHandler<T> getResultHandler() {
        return new EmptyServiceResultHandler<T>();
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return new DefaultServiceErrorChecker(logger);
    }

    protected Path getWorkingDirectory(JacsServiceData jacsServiceData) {
        if (StringUtils.isNotBlank(jacsServiceData.getWorkspace())) {
            return Paths.get(jacsServiceData.getWorkspace());
        } else if (StringUtils.isNotBlank(defaultWorkingDir)) {
            return getServicePath(defaultWorkingDir, jacsServiceData);
        } else {
            return getServicePath(System.getProperty("java.io.tmpdir"), jacsServiceData);
        }
    }

    protected Map<String, Object> setOutputAndErrorPaths(JacsServiceData jacsServiceData) {
        Map<String, Object> serviceUpdates = new LinkedHashMap<>();
        if (StringUtils.isBlank(jacsServiceData.getOutputPath())) {
            jacsServiceData.setOutputPath(getServicePath(
                    getWorkingDirectory(jacsServiceData).toString(),
                    jacsServiceData,
                    String.format("%s-stdout.txt", jacsServiceData.getName(), jacsServiceData.hasId() ? "-" + jacsServiceData.getId() : "")).toString());
            serviceUpdates.put("outputPath", jacsServiceData.getOutputPath());
        }
        if (StringUtils.isBlank(jacsServiceData.getErrorPath())) {
            jacsServiceData.setErrorPath(getServicePath(
                    getWorkingDirectory(jacsServiceData).toString(),
                    jacsServiceData,
                    String.format("%s-stderr.txt", jacsServiceData.getName(), jacsServiceData.hasId() ? "-" + jacsServiceData.getId() : "")).toString());
            serviceUpdates.put("errorPath", jacsServiceData.getErrorPath());
        }
        jacsServiceDataPersistence.update(jacsServiceData, serviceUpdates);
        return serviceUpdates;
    }

    protected Path getServicePath(String baseDir, JacsServiceData jacsServiceData, String... more) {
        ImmutableList.Builder<String> pathElemsBuilder = ImmutableList.<String>builder()
                .add(jacsServiceData.getName());
        if (jacsServiceData.hasId()) {
            pathElemsBuilder.addAll(FileUtils.getTreePathComponentsForId(jacsServiceData.getId()));
        }
        pathElemsBuilder.addAll(Arrays.asList(more));
        return Paths.get(baseDir, pathElemsBuilder.build().toArray(new String[0])).toAbsolutePath();
    }

    protected JacsServiceData submitDependencyIfNotFound(JacsServiceData dependency) {
        return jacsServiceDataPersistence.createServiceIfNotFound(dependency);
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

    protected boolean areAllDependenciesDone(JacsServiceData jacsServiceData) {
        return areAllDependenciesDoneFunc().apply(jacsServiceData).getResult();
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

    protected JacsServiceResult<T> updateServiceResult(JacsServiceData jacsServiceData, T result) {
        this.getResultHandler().updateServiceDataResult(jacsServiceData, result);
        jacsServiceDataPersistence.updateServiceResult(jacsServiceData);
        return new JacsServiceResult<>(jacsServiceData, result);
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
}

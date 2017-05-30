package org.janelia.jacs2.asyncservice.common;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.janelia.jacs2.model.jacsservice.ProcessingLocation;
import org.slf4j.Logger;

import javax.enterprise.inject.Instance;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractExeBasedServiceProcessor<S, T> extends AbstractBasicLifeCycleServiceProcessor<S, T> {

    protected static final String DY_LIBRARY_PATH_VARNAME = "LD_LIBRARY_PATH";

    private final String executablesBaseDir;
    private final Instance<ExternalProcessRunner> serviceRunners;
    private final ThrottledProcessesQueue throttledProcessesQueue;
    private final ApplicationConfig applicationConfig;

    public AbstractExeBasedServiceProcessor(ServiceComputationFactory computationFactory,
                                            JacsServiceDataPersistence jacsServiceDataPersistence,
                                            Instance<ExternalProcessRunner> serviceRunners,
                                            String defaultWorkingDir,
                                            ThrottledProcessesQueue throttledProcessesQueue,
                                            ApplicationConfig applicationConfig,
                                            Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.serviceRunners = serviceRunners;
        this.executablesBaseDir = applicationConfig.getStringPropertyValue("Executables.ModuleBase");
        this.throttledProcessesQueue = throttledProcessesQueue;
        this.applicationConfig = applicationConfig;
    }

    @Override
    protected ServiceComputation<JacsServiceResult<S>> processing(JacsServiceResult<S> depsResult) {
        ExeJobInfo jobInfo = runExternalProcess(depsResult.getJacsServiceData());
        return computationFactory.newCompletedComputation(depsResult)
                .thenSuspendUntil(pd -> new ContinuationCond.Cond<>(pd, this.hasJobFinished(pd.getJacsServiceData(), jobInfo)))
                .thenApply(pdCond -> {
                    JacsServiceResult<S> pd = pdCond.getState();
                    List<String> errors = this.getErrorChecker().collectErrors(pd.getJacsServiceData());
                    String errorMessage = null;
                    if (CollectionUtils.isNotEmpty(errors)) {
                        errorMessage = String.format("Process %s failed; errors found: %s", jobInfo.getScriptName(), String.join(";", errors));
                    } else if (jobInfo.hasFailed()) {
                        errorMessage = String.format("Process %s failed", jobInfo.getScriptName());
                    }
                    if (errorMessage != null) {
                        jacsServiceDataPersistence.updateServiceState(
                                pd.getJacsServiceData(),
                                JacsServiceState.ERROR,
                                Optional.of(JacsServiceData.createServiceEvent(JacsServiceEventTypes.FAILED, errorMessage)));
                        throw new ComputationException(pd.getJacsServiceData(), errorMessage);
                    }
                    return pd;
                });
    }

    protected boolean hasJobFinished(JacsServiceData jacsServiceData, ExeJobInfo jobInfo) {
        if (jobInfo.isDone()) {
            return true;
        }
        try {
            verifyAndFailIfTimeOut(jacsServiceData);
        } catch (ComputationException e) {
            jobInfo.terminate();
            throw e;
        }
        return false;
    }

    protected abstract ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData);

    protected abstract Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData);

    protected Optional<String> getEnvVar(String varName) {
        return Optional.ofNullable(System.getenv(varName));
    }

    protected ApplicationConfig getApplicationConfig() {
        return applicationConfig;
    }

    protected String getFullExecutableName(String... execPathComponents) {
        String baseDir;
        String[] pathComponents;
        if (execPathComponents.length > 0 && StringUtils.startsWith(execPathComponents[0], "/")) {
            baseDir = execPathComponents[0];
            pathComponents = Arrays.copyOfRange(execPathComponents, 1, execPathComponents.length);
        } else {
            baseDir = executablesBaseDir;
            pathComponents = execPathComponents;
        }
        Path cmdPath;
        if (StringUtils.isNotBlank(baseDir)) {
            cmdPath = Paths.get(baseDir, pathComponents);
        } else {
            cmdPath = Paths.get("", execPathComponents);
        }
        return cmdPath.toString();
    }

    protected String getUpdatedEnvValue(String varName, String addedValue) {
        Preconditions.checkArgument(StringUtils.isNotBlank(addedValue), "Cannot update environment variable " + varName + " with a null or empty value");
        Optional<String> currentValue = getEnvVar(varName);
        if (currentValue.isPresent()) {
            // prepend the new value
            return addedValue + ":" + currentValue.get();
        } else {
            return addedValue;
        }
    }

    protected ExeJobInfo runExternalProcess(JacsServiceData jacsServiceData) {
        ExternalCodeBlock script = prepareExternalScript(jacsServiceData);
        Map<String, String> env = prepareEnvironment(jacsServiceData);
        int defaultMaxRunningProcesses = applicationConfig.getIntegerPropertyValue("service.maxRunningProcesses", -1);
        int maxRunningProcesses = applicationConfig.getIntegerPropertyValue(
                "service." + jacsServiceData.getName() + ".maxRunningProcesses",
                defaultMaxRunningProcesses);
        ExternalProcessRunner processRunner =
                new ThrottledExternalProcessRunner(throttledProcessesQueue, jacsServiceData.getName(), getProcessRunner(jacsServiceData.getProcessingLocation()), maxRunningProcesses);
        return processRunner.runCmds(
                script,
                env,
                getWorkingDirectory(jacsServiceData).toString(),
                jacsServiceData);
    }

    private ExternalProcessRunner getProcessRunner(ProcessingLocation processingLocation) {
        ProcessingLocation location = processingLocation == null ? ProcessingLocation.LOCAL : processingLocation;
        for (ExternalProcessRunner serviceRunner : serviceRunners) {
            if (serviceRunner.supports(location)) {
                return serviceRunner;
            }
        }
        throw new IllegalArgumentException("Unsupported runner: " + processingLocation);
    }

}

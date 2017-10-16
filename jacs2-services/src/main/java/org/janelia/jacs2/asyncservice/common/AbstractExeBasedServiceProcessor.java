package org.janelia.jacs2.asyncservice.common;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
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
import java.util.*;

@MdcContext
public abstract class AbstractExeBasedServiceProcessor<R> extends AbstractBasicLifeCycleServiceProcessor<R, Void> {

    protected static final String DY_LIBRARY_PATH_VARNAME = "LD_LIBRARY_PATH";

    private final String executablesBaseDir;
    private final Instance<ExternalProcessRunner> serviceRunners;
    private final ThrottledProcessesQueue throttledProcessesQueue;
    private final ApplicationConfig applicationConfig;
    private final int jobIntervalCheck;

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
        this.jobIntervalCheck = applicationConfig.getIntegerPropertyValue("service.exejob.checkIntervalInMillis", 0);
    }

    @Override
    protected ServiceComputation<JacsServiceResult<Void>> processing(JacsServiceResult<Void> depsResult) {
        ExeJobInfo jobInfo = runExternalProcess(depsResult.getJacsServiceData());
        PeriodicallyCheckableState<JacsServiceResult<Void>> periodicResultCheck = new PeriodicallyCheckableState<>(depsResult, jobIntervalCheck);
        return computationFactory.newCompletedComputation(periodicResultCheck)
                .thenSuspendUntil((PeriodicallyCheckableState<JacsServiceResult<Void>> state) -> new ContinuationCond.Cond<>(state,
                        periodicResultCheck.updateCheckTime() && hasJobFinished(periodicResultCheck.getState().getJacsServiceData(), jobInfo)))
                .thenApply(pdCond -> {
                    JacsServiceResult<Void> pd = pdCond.getState().getState();
                    List<String> errors = getErrors(pd.getJacsServiceData());
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

    protected void prepareResources(JacsServiceData jacsServiceData) {
        // the method updates service resources mainly for setting up the grid resource limits.
    }

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
        if (StringUtils.isBlank(addedValue))  {
            return "";
        }
        return getEnvVar(varName)
                .map(currentValue -> addedValue + ":" + currentValue)
                .orElse(addedValue)
                ;
    }

    protected String getScriptDirName(JacsServiceData jacsServiceData) {
        return getWorkingDirectory(jacsServiceData).toString();
    }

    protected String getProcessDirName(JacsServiceData jacsServiceData) {
        return getWorkingDirectory(jacsServiceData).toString();
    }

    protected ExeJobInfo runExternalProcess(JacsServiceData jacsServiceData) {
        List<ExternalCodeBlock> externalConfigs = prepareConfigurationFiles(jacsServiceData);
        ExternalCodeBlock script = prepareExternalScript(jacsServiceData);
        Map<String, String> env = prepareEnvironment(jacsServiceData);
        prepareResources(jacsServiceData);
        int defaultMaxRunningProcesses = applicationConfig.getIntegerPropertyValue("service.maxRunningProcesses", -1);
        int maxRunningProcesses = applicationConfig.getIntegerPropertyValue(
                "service." + jacsServiceData.getName() + ".maxRunningProcesses",
                defaultMaxRunningProcesses);
        ExternalProcessRunner processRunner =
                new ThrottledExternalProcessRunner(throttledProcessesQueue, jacsServiceData.getName(), getProcessRunner(jacsServiceData), maxRunningProcesses);
        return processRunner.runCmds(
                script,
                externalConfigs,
                env,
                getScriptDirName(jacsServiceData),
                getProcessDirName(jacsServiceData),
                jacsServiceData);
    }

    protected List<ExternalCodeBlock> prepareConfigurationFiles(JacsServiceData jacsServiceData) {
        return Collections.emptyList();
    }

    private ExternalProcessRunner getProcessRunner(JacsServiceData jacsServiceData) {
        ProcessingLocation location = jacsServiceData.getProcessingLocation();
        if (location == null) {
            // if processing location is not set, use a default location (if needed it can be service specific)
            String defaultProcessingLocation = applicationConfig.getStringPropertyValue("service.defaultProcessingLocation", ProcessingLocation.LOCAL.name());
            String defaultServiceProcessingLocation = applicationConfig.getStringPropertyValue(
                    "service." + jacsServiceData.getName() + ".defaultProcessingLocation",
                    defaultProcessingLocation);
            try {
                location = ProcessingLocation.valueOf(defaultServiceProcessingLocation);
            } catch (Exception e) {
                logger.warn("Invalid default service processing location: {} / {} - defaulting to LOCAL", defaultProcessingLocation, defaultServiceProcessingLocation, e);
                location = ProcessingLocation.LOCAL; // default to local if something is miss configured
            }
        }
        for (ExternalProcessRunner serviceRunner : serviceRunners) {
            if (serviceRunner.supports(location)) {
                return serviceRunner;
            }
        }
        throw new IllegalArgumentException("Unsupported runner: " + location);
    }

}

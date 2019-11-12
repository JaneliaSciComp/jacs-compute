package org.janelia.jacs2.asyncservice.common;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.enterprise.inject.Instance;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.asyncservice.lsmfileservices.LsmFileMetadataProcessor;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsJobInstanceInfo;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ProcessingLocation;
import org.slf4j.Logger;

@MdcContext
public abstract class AbstractExeBasedServiceProcessor<R> extends AbstractServiceProcessor<R> {

    protected static final String DY_LIBRARY_PATH_VARNAME = "LD_LIBRARY_PATH";
    protected static final String LIBPATH_SEPARATOR = ":";

    private final String executablesBaseDir;
    private final Instance<ExternalProcessRunner> serviceRunners;
    private final JacsJobInstanceInfoDao jacsJobInstanceInfoDao;
    private final ApplicationConfig applicationConfig;
    private final int jobIntervalCheck;

    public AbstractExeBasedServiceProcessor(ServiceComputationFactory computationFactory,
                                            JacsServiceDataPersistence jacsServiceDataPersistence,
                                            Instance<ExternalProcessRunner> serviceRunners,
                                            String defaultWorkingDir,
                                            JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                                            ApplicationConfig applicationConfig,
                                            Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.serviceRunners = serviceRunners;
        this.executablesBaseDir = applicationConfig.getStringPropertyValue("Executables.ModuleBase");
        this.jacsJobInstanceInfoDao = jacsJobInstanceInfoDao;
        this.applicationConfig = applicationConfig;
        this.jobIntervalCheck = applicationConfig.getIntegerPropertyValue("service.exejob.checkIntervalInMillis", 0);
    }

    protected void prepareProcessing(JacsServiceData jacsServiceData) {
        updateOutputAndErrorPaths(jacsServiceData);
    }

    @Override
    public ServiceComputation<JacsServiceResult<R>> process(JacsServiceData jacsServiceData) {
        prepareProcessing(jacsServiceData);
        ExeJobHandler jobHandler = runExternalProcess(jacsServiceData);
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenSuspendUntil(sd -> new ContinuationCond.Cond<>(sd, hasJobFinished(sd, jobHandler)),
                        (long) jobIntervalCheck,
                        null)
                .thenApply(sd -> {
                    // Persist all final job instance metadata
                    Collection<JacsJobInstanceInfo> completedJobInfos = jobHandler.getJobInstances();
                    if (!completedJobInfos.isEmpty()) {
                        for (JacsJobInstanceInfo jacsJobInstanceInfo : completedJobInfos) {
                            jacsJobInstanceInfo.setServiceDataId(jacsServiceData.getId());
                        }
                        logger.trace("Saving {} job instance info objects", completedJobInfos.size());
                        jacsJobInstanceInfoDao.saveAll(completedJobInfos);
                    }

                    List<String> errors = getErrors(jacsServiceData);
                    String errorMessage;
                    if (CollectionUtils.isNotEmpty(errors)) {
                        errorMessage = String.format("Process %s failed; errors found: %s", jobHandler.getJobInfo(), String.join(";", errors));
                    } else if (jobHandler.hasFailed()) {
                        errorMessage = String.format("Process %s failed", jobHandler.getJobInfo());
                    } else {
                        errorMessage = null;
                    }
                    if (errorMessage != null) {
                        jacsServiceDataPersistence.updateServiceState(
                                jacsServiceData,
                                JacsServiceState.ERROR,
                                JacsServiceData.createServiceEvent(JacsServiceEventTypes.FAILED, errorMessage));
                        throw new ComputationException(jacsServiceData, errorMessage);
                    }
                    return updateServiceResult(sd, getResultHandler().collectResult(sd));
                })
                .thenApply(sr -> postProcessing(sr));
    }

    private boolean hasJobFinished(JacsServiceData jacsServiceData, ExeJobHandler jobHandler) {
        JacsServiceData updatedServiceData = refreshServiceData(jacsServiceData);
        // if the service has been canceled but the job hasn't finished terminate the job
        // if the service has been suspended let the job complete
        // so there's no need to do anything here
        if (updatedServiceData.hasBeenCanceled()) {
            if (!jobHandler.isDone()) {
                jobHandler.terminate();
            }
            throw new ComputationException(jacsServiceData, "Terminate service " + jacsServiceData.getId());
        } else if (jobHandler.isDone()) {
            return true;
        }
        try {
            verifyAndFailIfTimeOut(jacsServiceData);
        } catch (ComputationException e) {
            jobHandler.terminate();
            throw e;
        }
        return false;
    }

    private JacsServiceData refreshServiceData(JacsServiceData jacsServiceData) {
        return jacsServiceData.hasId() ? jacsServiceDataPersistence.findById(jacsServiceData.getId()) : jacsServiceData;
    }

    protected abstract ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData);

    /**
     * Override this to add environment variables which should exist when the script is run. The default implementation
     * returns an empty map.
     * @param jacsServiceData
     * @return
     */
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        return ImmutableMap.of();
    }

    /**
     * Override this to set up the execution resources in the JacsServiceData, e.g. for the cluster.
     * @param jacsServiceData
     */
    protected void prepareResources(JacsServiceData jacsServiceData) {
    }

    private Optional<String> getEnvVar(String varName) {
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
                .map(currentValue -> addedValue + LIBPATH_SEPARATOR + currentValue)
                .orElse(addedValue)
                ;
    }

    private JacsServiceFolder getScriptDirName(JacsServiceData jacsServiceData) {
        return getWorkingDirectory(jacsServiceData);
    }

    protected Path getProcessDir(JacsServiceData jacsServiceData) {
        return getWorkingDirectory(jacsServiceData).getServiceFolder();
    }

    private ExeJobHandler runExternalProcess(JacsServiceData jacsServiceData) {
        List<ExternalCodeBlock> externalConfigs = prepareConfigurationFiles(jacsServiceData);
        ExternalCodeBlock script = prepareExternalScript(jacsServiceData);
        Map<String, String> runtimeEnv = new LinkedHashMap<>();
        runtimeEnv.putAll(prepareEnvironment(jacsServiceData));
        if (MapUtils.isNotEmpty(jacsServiceData.getEnv())) {
            runtimeEnv.putAll(jacsServiceData.getEnv());
        }
        prepareResources(jacsServiceData);
        ExternalProcessRunner processRunner = getProcessRunner(jacsServiceData);
        return processRunner.runCmds(
                script,
                externalConfigs,
                runtimeEnv,
                getScriptDirName(jacsServiceData),
                getProcessDir(jacsServiceData),
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

    protected JacsServiceResult<R> postProcessing(JacsServiceResult<R> sr) {
        return sr;
    }

}

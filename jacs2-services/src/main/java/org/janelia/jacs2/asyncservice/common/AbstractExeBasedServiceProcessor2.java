package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.service.*;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@MdcContext
public abstract class AbstractExeBasedServiceProcessor2<R> extends AbstractBasicLifeCycleServiceProcessor2<R> {

    protected static final String DY_LIBRARY_PATH_VARNAME = "LD_LIBRARY_PATH";

    @Inject @PropertyValue(name = "Executables.ModuleBase")
    private String executablesBaseDir;

    @Inject @IntPropertyValue(name = "service.exejob.checkIntervalInMillis")
    private int jobIntervalCheck;

    @Inject
    @Any
    private Instance<ExternalProcessRunner> serviceRunners;

    @Inject
    private JacsJobInstanceInfoDao jacsJobInstanceInfoDao;

    @Inject
    private ApplicationConfig applicationConfig;

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData sd) throws Exception {
        JacsServiceData jacsServiceDataHierarchy = super.prepareProcessing(sd);
        updateOutputAndErrorPaths(jacsServiceDataHierarchy);
        return jacsServiceDataHierarchy;
    }

    private void updateOutputAndErrorPaths(JacsServiceData jacsServiceData) {
        Map<String, EntityFieldValueHandler<?>> serviceUpdates = new LinkedHashMap<>();
        JacsServiceFolder jacsServiceFolder = getWorkingDirectory(jacsServiceData);
        if (StringUtils.isBlank(jacsServiceData.getOutputPath())) {
            jacsServiceData.setOutputPath(jacsServiceFolder.getServiceFolder(JacsServiceFolder.SERVICE_OUTPUT_DIR).toString());
            serviceUpdates.put("outputPath", new SetFieldValueHandler<>(jacsServiceData.getOutputPath()));
        }
        if (StringUtils.isBlank(jacsServiceData.getErrorPath())) {
            jacsServiceData.setErrorPath(jacsServiceFolder.getServiceFolder(JacsServiceFolder.SERVICE_ERROR_DIR).toString());
            serviceUpdates.put("errorPath", new SetFieldValueHandler<>(jacsServiceData.getErrorPath()));
        }
        jacsServiceDataPersistence.update(jacsServiceData, serviceUpdates);
    }

    @Override
    protected ServiceComputation<JacsServiceData> processing(JacsServiceData sd) {
        ExeJobInfo jobInfo = runExternalProcess(sd);
        PeriodicallyCheckableState<JacsServiceData> periodicResultCheck = new PeriodicallyCheckableState<>(sd, jobIntervalCheck);
        return computationFactory.newCompletedComputation(periodicResultCheck)
                .thenSuspendUntil((PeriodicallyCheckableState<JacsServiceData> state) -> new ContinuationCond.Cond<>(state,
                        periodicResultCheck.updateCheckTime() && hasJobFinished(periodicResultCheck.getState(), jobInfo)))
                .thenApply(pdCond -> {

                    JacsServiceData jacsServiceData = pdCond.getState();

                    // Persist all final job instance metadata
                    Collection<JacsJobInstanceInfo> completedJobInfos = jobInfo.getJobInstanceInfos();
                    if (!completedJobInfos.isEmpty()) {
                        for (JacsJobInstanceInfo jacsJobInstanceInfo : completedJobInfos) {
                            jacsJobInstanceInfo.setServiceDataId(jacsServiceData.getId());
                        }
                        logger.trace("Saving {} job instance info objects", completedJobInfos.size());
                        jacsJobInstanceInfoDao.saveAll(completedJobInfos);
                    }

                    List<String> errors = getErrors(jacsServiceData);
                    String errorMessage = null;
                    if (CollectionUtils.isNotEmpty(errors)) {
                        errorMessage = String.format("Process %s failed; errors found: %s", jobInfo.getScriptName(), String.join(";", errors));
                    } else if (jobInfo.hasFailed()) {
                        errorMessage = String.format("Process %s failed", jobInfo.getScriptName());
                    }
                    if (errorMessage != null) {
                        jacsServiceDataPersistence.updateServiceState(
                                jacsServiceData,
                                JacsServiceState.ERROR,
                                JacsServiceData.createServiceEvent(JacsServiceEventTypes.FAILED, errorMessage));
                        throw new ComputationException(jacsServiceData, errorMessage);
                    }
                    return sd;
                });
    }

    protected List<String> getErrors(JacsServiceData jacsServiceData) {
        return this.getErrorChecker().collectErrors(jacsServiceData);
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return new CoreDumpServiceErrorChecker(logger);
    }

    private boolean hasJobFinished(JacsServiceData jacsServiceData, ExeJobInfo jobInfo) {
        JacsServiceData updatedServiceData = refreshServiceData(jacsServiceData);
        // if the service has been canceled but the job hasn't finished terminate the job
        // if the service has been suspended let the job complete
        // so there's no need to do anything here
        if (updatedServiceData.hasBeenCanceled()) {
            if (!jobInfo.isDone()) {
                jobInfo.terminate();
            }
            throw new ComputationException(jacsServiceData, "Terminate service " + jacsServiceData.getId());
        } else if (jobInfo.isDone()) {
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

    private JacsServiceData refreshServiceData(JacsServiceData jacsServiceData) {
        return jacsServiceData.hasId() ? jacsServiceDataPersistence.findById(jacsServiceData.getId()) : jacsServiceData;
    }

    private void verifyAndFailIfTimeOut(JacsServiceData jacsServiceData) {
        long timeSinceStart = System.currentTimeMillis() - jacsServiceData.getProcessStartTime().getTime();
        if (jacsServiceData.timeout() > 0 && timeSinceStart > jacsServiceData.timeout()) {
            jacsServiceDataPersistence.updateServiceState(
                    jacsServiceData,
                    JacsServiceState.TIMEOUT,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.TIMEOUT, String.format("Service timed out after %s ms", timeSinceStart)));
            logger.warn("Service {} timed out after {}ms", jacsServiceData, timeSinceStart);
            throw new ComputationException(jacsServiceData, "Service " + jacsServiceData.getId() + " timed out");
        }
    }

    protected ExternalCodeBlock prepareExternalScript() {
        try {
            ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
            ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
            createScript(externalScriptWriter);
            externalScriptWriter.close();
            return externalScriptCode;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Override this to create the script that is run on the cluster.
     * @param scriptWriter script writer which will be written to a file
     * @throws IOException if there is a problem writing to the file
     */
    protected abstract void createScript(ScriptWriter scriptWriter) throws IOException;

    /**
     * Override this to add environment variables which should exist when the script is run. The default implementation
     * returns an empty map.
     * @return map of environment variables for the executable process
     */
    protected Map<String, String> prepareEnvironment() {
        return ImmutableMap.of();
    }

    /**
     * Override this to set up the execution resources in the JacsServiceData, e.g. for the cluster.
     * @param jacsServiceData
     */
    protected void prepareResources(JacsServiceData jacsServiceData) {

        Map<String, String> jobResources = jacsServiceData.getResources();

        Integer requiredSlots = getRequiredSlots();
        if (requiredSlots != null) {
            ProcessorHelper.setRequiredSlots(jobResources, requiredSlots);
        }

        Integer requiredMemoryInGB = getRequiredMemoryInGB();
        if (requiredMemoryInGB!=null) {
            ProcessorHelper.setRequiredMemoryInGB(jobResources, requiredMemoryInGB);
        }

        Integer hardRuntimeLimitSeconds = getHardRuntimeLimitSeconds();
        if (hardRuntimeLimitSeconds!=null) {
            ProcessorHelper.setHardJobDurationLimitInSeconds(jobResources, hardRuntimeLimitSeconds);
        }

        Integer softRuntimeLimitSeconds = getSoftRuntimeLimitSeconds();
        if (softRuntimeLimitSeconds!=null) {
            ProcessorHelper.setSoftJobDurationLimitInSeconds(jobResources, softRuntimeLimitSeconds);
        }
    }

    private Optional<String> getEnvVar(String varName) {
        return Optional.ofNullable(System.getenv(varName));
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

    private JacsServiceFolder getScriptDirName(JacsServiceData jacsServiceData) {
        return getWorkingDirectory(jacsServiceData);
    }

    protected Path getProcessDir(JacsServiceData jacsServiceData) {
        return getWorkingDirectory(jacsServiceData).getServiceFolder();
    }

    private ExeJobInfo runExternalProcess(JacsServiceData jacsServiceData) {
        List<ExternalCodeBlock> externalConfigs = prepareConfigurationFiles();
        ExternalCodeBlock script = prepareExternalScript();
        Map<String, String> runtimeEnv = new LinkedHashMap<>();
        runtimeEnv.putAll(prepareEnvironment());
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

    protected List<ExternalCodeBlock> prepareConfigurationFiles() {
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
            logger.trace("Checking {} for {}", serviceRunner, location);
            if (serviceRunner.supports(location)) {
                logger.trace("Matched");
                return serviceRunner;
            }
        }
        throw new IllegalArgumentException("Unsupported runner: " + location);
    }

    /**
     * Override this method to specify the minimum number of slots needed for this grid job. At least this many slots
     * will be allocated, but more slots may be allocated to fulfill the memory requirement provided by
     * getRequiredMemoryInGB.
     *
     * Defaults to 1 slot.
     *
     * @return the minimum number of slots needed for this grid job.
     */
    protected Integer getRequiredSlots() {
        return null;
    }

    /**
     * Override this method to specify the minimum amount of memory needed for this grid job. Enough slots will be
     * allocated to achieve this memory requirement.
     *
     * Defaults to 1 GB.
     *
     * @return the minimum amount of memory needed for this grid job.
     */
    protected Integer getRequiredMemoryInGB() {
        return null;
    }

    /**
     * Override this method to specify a hard runtime limit in seconds. This method returns null by default, meaning that
     * no hard runtime limit will be added.
     * @return
     */
    protected Integer getHardRuntimeLimitSeconds() {
        return null;
    }

    /**
     * Override this method to specify a soft runtime limit in seconds. This method returns null by default, meaning that
     * no hard runtime limit will be added.
     * @return
     */
    protected Integer getSoftRuntimeLimitSeconds() {
        return null;
    }

}

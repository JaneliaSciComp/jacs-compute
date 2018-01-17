package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.janelia.jacs2.asyncservice.qualifier.LocalJob;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@LocalJob
public class ExternalLocalProcessRunner extends AbstractExternalProcessRunner {

    private final ThrottledExeJobsQueue jobsQueue;
    private final ApplicationConfig applicationConfig;

    @Inject
    public ExternalLocalProcessRunner(JacsServiceDataPersistence jacsServiceDataPersistence, ThrottledExeJobsQueue jobsQueue, ApplicationConfig applicationConfig, Logger logger) {
        super(jacsServiceDataPersistence, logger);
        this.jobsQueue = jobsQueue;
        this.applicationConfig = applicationConfig;
    }

    @Override
    public ExeJobInfo runCmds(ExternalCodeBlock externalCode,
                              List<ExternalCodeBlock> externalConfigs,
                              Map<String, String> env,
                              JacsServiceFolder scriptServiceFolder,
                              Path processDir,
                              JacsServiceData serviceContext) {
        logger.debug("Begin local process invocation for {}", serviceContext);
        jacsServiceDataPersistence.updateServiceState(serviceContext, JacsServiceState.RUNNING, Optional.empty());
        String processingScript = scriptServiceFolder.getServiceFolder("<unknown>").toString();
        try {
            processingScript = createProcessingScript(externalCode, env, scriptServiceFolder, JacsServiceFolder.SERVICE_CONFIG_DIR);
            File processDirectory = processDir.toFile();
            Files.createDirectories(processDir);

            List<File> configFiles = createConfigFiles(externalConfigs, scriptServiceFolder, JacsServiceFolder.SERVICE_CONFIG_DIR);

            File outputFile = prepareOutputFile(serviceContext.getOutputPath(), "Output file must be set before running the service " + serviceContext.getName());
            File errorFile = prepareOutputFile(serviceContext.getErrorPath(), "Error file must be set before running the service " + serviceContext.getName());

            int defaultMaxRunningProcesses = applicationConfig.getIntegerPropertyValue("service.maxRunningProcesses", -1);
            int maxRunningProcesses = applicationConfig.getIntegerPropertyValue(
                    "service." + serviceContext.getName() + ".maxRunningProcesses",
                    defaultMaxRunningProcesses);

            ExeJobInfo jobInfo;
            if (CollectionUtils.size(externalConfigs) <= 1) {
                jobInfo = runSingleProcess(processingScript, processDirectory, configFiles.stream().findFirst().orElse(null), outputFile, errorFile, env, maxRunningProcesses, serviceContext);
            } else {
                jobInfo = runMultipleProcessess(processingScript, processDirectory, configFiles, outputFile, errorFile, env, maxRunningProcesses, serviceContext);
            }
            String jobId = jobInfo.start();
            // start the job
            jacsServiceDataPersistence.addServiceEvent(
                    serviceContext,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.START_PROCESS, String.format("Start %s [%s] %s", processingScript, jobId, configFiles))
            );
            return jobInfo;
        } catch (Exception e) {
            logger.error("Error starting the computation process {} for {}", processingScript, serviceContext, e);
            jacsServiceDataPersistence.updateServiceState(
                    serviceContext,
                    JacsServiceState.ERROR,
                    Optional.of(JacsServiceData.createServiceEvent(JacsServiceEventTypes.START_PROCESS_ERROR, String.format("Error starting %s - %s", processingScript, e.getMessage())))
            );
            throw new ComputationException(serviceContext, e);
        }
    }

    private ExeJobInfo runSingleProcess(String processingScript,
                                        File processDirectory,
                                        File processInput,
                                        File processOutput,
                                        File processError,
                                        Map<String, String> env,
                                        int maxRunningProcesses,
                                        JacsServiceData serviceContext) {
        ProcessBuilder processBuilder = new ProcessBuilder(ImmutableList.<String>builder()
                .add(processingScript)
                .build());
        if (MapUtils.isNotEmpty(env)) {
            processBuilder.environment().putAll(env);
        }

        String withConfigOption;
        if (processInput != null) {
            processBuilder.redirectInput(processInput);
            withConfigOption = " with " + processInput.toString() + " ";
        } else {
            withConfigOption = "";
        }

        // set the working directory, the process stdout and stderr
        processBuilder.directory(processDirectory)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(processOutput))
                .redirectError(ProcessBuilder.Redirect.appendTo(processError));

        logger.info("Start {} in {}{} for {} using env {}", processingScript, processDirectory, withConfigOption, serviceContext, processBuilder.environment());
        return new ThrottledJobInfo(new LocalExeJobInfo(processBuilder, processingScript), serviceContext, jobsQueue, maxRunningProcesses);
    }

    private ExeJobInfo runMultipleProcessess(String processingScript,
                                             File processDirectory,
                                             List<File> processInputs,
                                             File processOutput,
                                             File processError,
                                             Map<String, String> env,
                                             int maxRunningProcesses,
                                             JacsServiceData serviceContext) {
        List<ExeJobInfo> jobList = processInputs.stream()
                .map(processInput -> runSingleProcess(processingScript, processDirectory, processInput, processOutput, processError, env, maxRunningProcesses, serviceContext))
                .collect(Collectors.toList());
        return new BatchJobInfo(jobList, processingScript);
    }
}

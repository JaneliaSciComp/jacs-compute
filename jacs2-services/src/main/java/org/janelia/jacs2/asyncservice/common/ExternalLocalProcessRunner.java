package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.janelia.jacs2.asyncservice.qualifier.LocalJob;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.jacs2.domain.IndexedReference;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEvent;
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
import java.util.stream.Collectors;

@LocalJob
public class ExternalLocalProcessRunner extends AbstractExternalProcessRunner {

    private final ThrottledExeJobsQueue jobsQueue;
    private final ApplicationConfig applicationConfig;

    @Inject
    public ExternalLocalProcessRunner(JacsServiceDataPersistence jacsServiceDataPersistence, ThrottledExeJobsQueue jobsQueue, @ApplicationProperties ApplicationConfig applicationConfig, Logger logger) {
        super(jacsServiceDataPersistence, logger);
        this.jobsQueue = jobsQueue;
        this.applicationConfig = applicationConfig;
    }

    @Override
    public ExeJobHandler runCmds(ExternalCodeBlock externalCode,
                                 List<ExternalCodeBlock> externalConfigs,
                                 Map<String, String> env,
                                 JacsServiceFolder scriptServiceFolder,
                                 Path processDir,
                                 JacsServiceData serviceContext) {
        logger.debug("Begin local process invocation for {}", serviceContext);
        jacsServiceDataPersistence.updateServiceState(serviceContext, JacsServiceState.RUNNING, JacsServiceEvent.NO_EVENT);
        String processingScript = scriptServiceFolder.getServiceFolder("<unknown>").toString();
        try {
            processingScript = createProcessingScript(externalCode, env, scriptServiceFolder, JacsServiceFolder.SERVICE_CONFIG_DIR);
            Files.createDirectories(processDir);

            List<File> configFiles = createConfigFiles(externalConfigs, scriptServiceFolder, JacsServiceFolder.SERVICE_CONFIG_DIR);

            prepareOutputDir(serviceContext.getOutputPath(), "Output directory must be set before running the service " + serviceContext.getName());
            prepareOutputDir(serviceContext.getErrorPath(), "Error directory must be set before running the service " + serviceContext.getName());

            int defaultMaxRunningProcesses = applicationConfig.getIntegerPropertyValue("service.maxRunningProcesses", -1);
            int maxRunningProcesses = applicationConfig.getIntegerPropertyValue(
                    "service." + serviceContext.getName() + ".maxRunningProcesses",
                    defaultMaxRunningProcesses);

            ExeJobHandler jobHandler;
            if (CollectionUtils.size(externalConfigs) < 1) {
                jobHandler = runSingleProcess(processingScript,
                        processDir,
                        null,
                        Paths.get(serviceContext.getOutputPath(), scriptServiceFolder.getServiceOutputPattern("")),
                        Paths.get(serviceContext.getErrorPath(), scriptServiceFolder.getServiceErrorPattern("")),
                        env,
                        maxRunningProcesses,
                        scriptServiceFolder);
            } else {
                jobHandler = runMultipleProcessess(processingScript,
                        processDir,
                        configFiles,
                        Paths.get(serviceContext.getOutputPath()),
                        Paths.get(serviceContext.getErrorPath()),
                        env,
                        maxRunningProcesses,
                        scriptServiceFolder);
            }
            jobHandler.start();
            // start the job
            jacsServiceDataPersistence.addServiceEvent(
                    serviceContext,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.START_PROCESS, String.format("Start %s [%s] %s", processingScript, jobHandler.getJobInfo(), configFiles))
            );
            return jobHandler;
        } catch (Exception e) {
            logger.error("Error starting the computation process {} for {}", processingScript, serviceContext, e);
            jacsServiceDataPersistence.updateServiceState(
                    serviceContext,
                    JacsServiceState.ERROR,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.START_PROCESS_ERROR, String.format("Error starting %s - %s", processingScript, e.getMessage()))
            );
            throw new ComputationException(serviceContext, e);
        }
    }

    private ExeJobHandler runSingleProcess(String processingScript,
                                           Path processDirectory,
                                           Path processInput,
                                           Path processOutput,
                                           Path processError,
                                           Map<String, String> env,
                                           int maxRunningProcesses,
                                           JacsServiceFolder scriptServiceFolder) {
        ProcessBuilder processBuilder = new ProcessBuilder(ImmutableList.<String>builder()
                .add(processingScript)
                .build());
        if (MapUtils.isNotEmpty(env)) {
            processBuilder.environment().putAll(env);
        }

        String withConfigOption;
        if (processInput != null) {
            processBuilder.redirectInput(processInput.toFile());
            withConfigOption = " with " + processInput.toString() + " ";
        } else {
            withConfigOption = "";
        }

        // set the working directory, the process stdout and stderr
        processBuilder.directory(processDirectory.toFile())
                .redirectOutput(ProcessBuilder.Redirect.appendTo(processOutput.toFile()))
                .redirectError(ProcessBuilder.Redirect.appendTo(processError.toFile()));

        logger.info("Start {} in {}{} for {} using env {}", processingScript, processDirectory, withConfigOption, scriptServiceFolder.getServiceData(), processBuilder.environment());
        return new ThrottledExeJobHandler(new LocalExeJobHandler(processingScript, processBuilder), scriptServiceFolder.getServiceData(), jobsQueue, maxRunningProcesses);
    }

    private ExeJobHandler runMultipleProcessess(String processingScript,
                                                Path processDirectory,
                                                List<File> processInputs,
                                                Path processOutput,
                                                Path processError,
                                                Map<String, String> env,
                                                int maxRunningProcesses,
                                                JacsServiceFolder scriptServiceFolder) {
        List<ExeJobHandler> jobList = IndexedReference.indexListContent(processInputs, (pos, processInputDir) -> new IndexedReference<>(processInputDir, pos + 1))
            .map(indexedInputDir -> runSingleProcess(processingScript,
                    processDirectory,
                    indexedInputDir.getReference().toPath(),
                    processOutput.resolve(scriptServiceFolder.getServiceOutputPattern("." + indexedInputDir.getPos())),
                    processError.resolve(scriptServiceFolder.getServiceErrorPattern("." + indexedInputDir.getPos())),
                    env,
                    maxRunningProcesses,
                    scriptServiceFolder))
            .collect(Collectors.toList());
        return new BatchExeJobHandler(processingScript, jobList);
    }
}

package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.MapUtils;
import org.janelia.jacs2.asyncservice.qualifier.LocalJob;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@LocalJob
public class ExternalLocalProcessRunner extends AbstractExternalProcessRunner {

    @Inject
    public ExternalLocalProcessRunner(JacsServiceDataPersistence jacsServiceDataPersistence, Logger logger) {
        super(jacsServiceDataPersistence, logger);
    }

    @Override
    public ExeJobInfo runCmds(ExternalCodeBlock externalCode,
                              List<ExternalCodeBlock> externalConfigs,
                              Map<String, String> env,
                              String scriptDirName,
                              String processDirName,
                              JacsServiceData serviceContext) {
        logger.debug("Begin local process invocation for {}", serviceContext);
        String processingScript = createProcessingScript(externalCode, env, scriptDirName, serviceContext);
        jacsServiceDataPersistence.updateServiceState(serviceContext, JacsServiceState.RUNNING, Optional.empty());
        File outputFile;
        File errorFile;
        try {
            File processDirectory = new File(processDirName);
            Files.createDirectories(processDirectory.toPath());
            outputFile = prepareOutputFile(serviceContext.getOutputPath(), "Output file must be set before running the service " + serviceContext.getName());
            errorFile = prepareOutputFile(serviceContext.getErrorPath(), "Error file must be set before running the service " + serviceContext.getName());
            ProcessBuilder processBuilder = new ProcessBuilder(ImmutableList.<String>builder()
                    .add(processingScript)
                    .build());
            if (MapUtils.isNotEmpty(env)) {
                processBuilder.environment().putAll(env);
            }
            // set the working directory, the process stdout and stderr
            processBuilder.directory(processDirectory)
                .redirectOutput(outputFile)
                .redirectError(errorFile);
            // start the local process
            Process localProcess;
            logger.info("Start {} for {} using  env {}", processingScript, serviceContext, processBuilder.environment());
            jacsServiceDataPersistence.addServiceEvent(
                    serviceContext,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.START_PROCESS, String.format("Start %s", processingScript))
            );
            localProcess = processBuilder.start();
            logger.info("Started process {} ({}) for {}", processingScript, localProcess, serviceContext);
            return new LocalExeJobInfo(localProcess, processingScript);
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

}

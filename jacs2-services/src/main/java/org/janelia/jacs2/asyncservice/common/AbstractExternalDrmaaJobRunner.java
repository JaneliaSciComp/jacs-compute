package org.janelia.jacs2.asyncservice.common;

import com.google.common.io.Files;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractExternalDrmaaJobRunner extends AbstractExternalProcessRunner {

    private final Session drmaaSession;

    AbstractExternalDrmaaJobRunner(Session drmaaSession, JacsServiceDataPersistence jacsServiceDataPersistence, Logger logger) {
        super(jacsServiceDataPersistence, logger);
        this.drmaaSession = drmaaSession;
    }

    @Override
    public ExeJobInfo runCmds(ExternalCodeBlock externalCode,
                              List<ExternalCodeBlock> externalConfigs,
                              Map<String, String> env,
                              String scriptDirName,
                              String processDirName,
                              JacsServiceData serviceContext) {
        logger.debug("Begin DRMAA job invocation for {}", serviceContext);
        jacsServiceDataPersistence.updateServiceState(serviceContext, JacsServiceState.RUNNING, Optional.empty());
        String processingScript = scriptDirName + "/<unknown>";
        JobTemplate jt = null;
        try {
            Path scriptsDir = FileUtils.createSubDirs(Paths.get(scriptDirName), "sge_config");
            processingScript = createProcessingScript(externalCode, env, scriptsDir.toString(), serviceContext);

            String configFilePattern = serviceContext.getName() + "Configuration.#";
            List<File> configFiles = createConfigFiles(externalConfigs, scriptsDir.toString(), configFilePattern, serviceContext);

            File outputFile = prepareOutputFile(serviceContext.getOutputPath(), "Output file must be set before running the service " + serviceContext.getName());
            File errorFile = prepareOutputFile(serviceContext.getErrorPath(), "Error file must be set before running the service " + serviceContext.getName());

            jt = drmaaSession.createJobTemplate();
            jt.setJobName(serviceContext.getName());
            jt.setRemoteCommand(processingScript);
            jt.setArgs(Collections.emptyList());
            File processDirectory = setJobWorkingDirectory(jt, processDirName);
            logger.debug("Using working directory {} for {}", processDirectory, serviceContext);
            jt.setJobEnvironment(env);
            if (CollectionUtils.size(externalConfigs) < 1) {
                if (StringUtils.isNotBlank(serviceContext.getInputPath())) {
                    jt.setInputPath(":" + serviceContext.getInputPath());
                }
            } else {
                jt.setInputPath(":" + scriptsDir.resolve(configFilePattern));
            }
            jt.setOutputPath(":" + outputFile.getAbsolutePath());
            jt.setErrorPath(":" + errorFile.getAbsolutePath());
            String nativeSpec = createNativeSpec(serviceContext.getResources());
            if (StringUtils.isNotBlank(nativeSpec)) {
                jt.setNativeSpecification(nativeSpec);
            }
            logger.info("Start {} for {} using  env {}", processingScript, serviceContext, env);
            ExeJobInfo jobInfo = new DrmaaJobInfo(drmaaSession, processingScript, configFiles.size(), jt);
            String withConfigOption;
            if (configFiles.size() > 0) {
                withConfigOption = " with " + configFiles + " ";
            } else {
                withConfigOption = "";
            }
            String jobId = jobInfo.start();
            jacsServiceDataPersistence.addServiceEvent(
                    serviceContext,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_SUBMIT, String.format("Submitted job %s {%s} running: %s%s", serviceContext.getName(), jobId, processingScript, withConfigOption))
            );
            return jobInfo;
        } catch (Exception e) {
            if (jt != null) {
                try {
                    drmaaSession.deleteJobTemplate(jt);
                } catch (DrmaaException drmaaExc) {
                    logger.error("Error deleting a DRMAA job {} for {}", processingScript, serviceContext, drmaaExc);
                }
            }
            jacsServiceDataPersistence.updateServiceState(
                    serviceContext,
                    JacsServiceState.ERROR,
                    Optional.of(JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_JOB_ERROR, String.format("Error creating DRMAA job %s - %s", serviceContext.getName(), e.getMessage())))
            );
            logger.error("Error creating a DRMAA job {} for {}", processingScript, serviceContext, e);
            throw new ComputationException(serviceContext, e);
        }
    }

    private File setJobWorkingDirectory(JobTemplate jt, String workingDirName) {
        File workingDirectory;
        if (StringUtils.isNotBlank(workingDirName)) {
            workingDirectory = new File(workingDirName);
        } else {
            workingDirectory = Files.createTempDir();
        }
        if (!workingDirectory.exists()) {
            workingDirectory.mkdirs();
        }
        if (!workingDirectory.exists()) {
            throw new IllegalStateException("Cannot create working directory " + workingDirectory.getAbsolutePath());
        }
        try {
            jt.setWorkingDirectory(workingDirectory.getAbsolutePath());
        } catch (DrmaaException e) {
            throw new IllegalStateException(e);
        }
        return workingDirectory;
    }

    protected abstract String createNativeSpec(Map<String, String> jobResources);

}

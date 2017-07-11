package org.janelia.jacs2.asyncservice.common;

import com.google.common.io.Files;
import org.apache.commons.lang3.StringUtils;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.slf4j.Logger;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractExternalDrmaaJobRunner extends AbstractExternalProcessRunner {

    private final Session drmaaSession;

    public AbstractExternalDrmaaJobRunner(Session drmaaSession, JacsServiceDataPersistence jacsServiceDataPersistence, Logger logger) {
        super(jacsServiceDataPersistence, logger);
        this.drmaaSession = drmaaSession;
    }

    @Override
    public ExeJobInfo runCmds(ExternalCodeBlock externalCode,
                              Map<String, String> env,
                              String workingDirName,
                              JacsServiceData serviceContext) {
        logger.debug("Begin DRMAA job invocation for {}", serviceContext);
        String processingScript = createProcessingScript(externalCode, workingDirName, serviceContext);
        jacsServiceDataPersistence.updateServiceState(serviceContext, JacsServiceState.RUNNING, Optional.empty());
        JobTemplate jt = null;
        File outputFile;
        File errorFile;
        try {
            jt = drmaaSession.createJobTemplate();
            jt.setJobName(serviceContext.getName());
            jt.setRemoteCommand(processingScript);
            jt.setArgs(Collections.emptyList());
            File workingDirectory = setJobWorkingDirectory(jt, workingDirName);
            logger.debug("Using working directory {} for {}", workingDirectory, serviceContext);
            jt.setJobEnvironment(env);
            if (StringUtils.isNotBlank(serviceContext.getInputPath())) {
                jt.setInputPath(":" + serviceContext.getInputPath());
            }
            outputFile = prepareOutputFile(serviceContext.getOutputPath(), "Output file must be set before running the service " + serviceContext.getName());
            jt.setOutputPath(":" + outputFile.getAbsolutePath());
            errorFile = prepareOutputFile(serviceContext.getErrorPath(), "Error file must be set before running the service " + serviceContext.getName());
            jt.setErrorPath(":" + errorFile.getAbsolutePath());
            Map<String, String> jobResources = serviceContext.getResources();
            String nativeSpec = createNativeSpec(jobResources);
            if (StringUtils.isNotBlank(nativeSpec)) {
                jt.setNativeSpecification(nativeSpec);
            }
            logger.info("Start {} for {} using  env {}", processingScript, serviceContext, env);
            String jobId = drmaaSession.runJob(jt);
            logger.info("Submitted job {} for {}", jobId, serviceContext);
            jacsServiceDataPersistence.addServiceEvent(
                    serviceContext,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.DRMAA_SUBMIT, String.format("Submitted job %s {%s} running: %s", serviceContext.getName(), jobId, processingScript))
            );
            drmaaSession.deleteJobTemplate(jt);
            jt = null;
            return new DrmaaJobInfo(drmaaSession, jobId, processingScript);
        } catch (Exception e) {
            jacsServiceDataPersistence.updateServiceState(
                    serviceContext,
                    JacsServiceState.ERROR,
                    Optional.of(JacsServiceData.createServiceEvent(JacsServiceEventTypes.DRMAA_JOB_ERROR, String.format("Error creating DRMAA job %s - %s", serviceContext.getName(), e.getMessage())))
            );
            logger.error("Error creating a DRMAA job {} for {}", processingScript, serviceContext, e);
            throw new ComputationException(serviceContext, e);
        } finally {
            if (jt != null) {
                try {
                    drmaaSession.deleteJobTemplate(jt);
                } catch (DrmaaException e) {
                    logger.error("Error deleting a DRMAA job {} for {}", processingScript, serviceContext, e);
                }
            }
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

    protected String getGridBillingAccount(Map<String, String> jobResources) {
        return jobResources.get("gridAccountId");
    }

    protected long getSoftJobDurationLimitInSeconds(Map<String, String> jobResources) {
        String jobDuration = StringUtils.defaultIfBlank(jobResources.get("softGridJobDurationInSeconds"), "-1");
        return Long.parseLong(jobDuration);
    }

    protected long getHardJobDurationLimitInSeconds(Map<String, String> jobResources) {
        String jobDuration = StringUtils.defaultIfBlank(jobResources.get("hardGridJobDurationInSeconds"), "-1");
        return Long.parseLong(jobDuration);
    }

    protected String getGridJobResourceLimits(Map<String, String> jobResources) {
        return jobResources.get("gridResourceLimits");
    }
}
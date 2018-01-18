package org.janelia.jacs2.asyncservice.common;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.apache.commons.lang3.StringUtils;
import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobTemplate;
import org.janelia.jacs2.asyncservice.common.cluster.LsfJavaJobInfo;
import org.janelia.jacs2.asyncservice.common.cluster.MonitoredJobManager;
import org.janelia.jacs2.asyncservice.qualifier.LSFJavaJob;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.GridExecutor;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * External runner which uses the java-lsf library to submit and manage cluster jobs.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@LSFJavaJob
public class ExternalLSFJavaJobRunner extends AbstractExternalProcessRunner {

    private final JobManager jobMgr;
    private final ExecutorService lsfJobExecutor;

    @Inject
    public ExternalLSFJavaJobRunner(MonitoredJobManager jobMgr,
                                    JacsServiceDataPersistence jacsServiceDataPersistence,
                                    @GridExecutor ExecutorService lsfJobExecutor,
                                    Logger logger) {
        super(jacsServiceDataPersistence, logger);
        this.jobMgr = jobMgr.getJobMgr();
        this.lsfJobExecutor = lsfJobExecutor;
    }

    @Override
    public ExeJobInfo runCmds(ExternalCodeBlock externalCode, List<ExternalCodeBlock> externalConfigs, Map<String, String> env, String scriptDirName, String processDirName, JacsServiceData serviceContext) {
        logger.debug("Begin bsub job invocation for {}", serviceContext);
        try {
            JobTemplate jt = prepareJobTemplate(externalCode, externalConfigs, env, processDirName, serviceContext);
            String processingScript = jt.getRemoteCommand();

            int numJobs = externalConfigs.isEmpty() ? 1 : externalConfigs.size();
            logger.info("Start {} for {} using  env {}", jt.getRemoteCommand(), serviceContext, env);
            LsfJavaJobInfo lsfJavaJobInfo = new LsfJavaJobInfo(jobMgr, jt, numJobs, processingScript, lsfJobExecutor);

            String jobId = lsfJavaJobInfo.start();
            logger.info("Submitted job {} for {}", jobId, serviceContext);

            jacsServiceDataPersistence.addServiceEvent(
                    serviceContext,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_SUBMIT, String.format("Submitted job %s {%s} running: %s", serviceContext.getName(), jobId, processingScript))
            );

            return lsfJavaJobInfo;
        } catch (Exception e) {
            jacsServiceDataPersistence.updateServiceState(
                    serviceContext,
                    JacsServiceState.ERROR,
                    Optional.of(JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_JOB_ERROR, String.format("Error creating DRMAA job %s - %s", serviceContext.getName(), e.getMessage())))
            );
            logger.error("Error creating a cluster job for {}", serviceContext, e);
            throw new ComputationException(serviceContext, e);
        }
    }

    @Override
    protected String createProcessingScript(ExternalCodeBlock externalCode, Map<String, String> env, String workingDirName, JacsServiceData sd) {
        ScriptWriter scriptWriter = null;
        try {
            Preconditions.checkArgument(!externalCode.isEmpty());
            Preconditions.checkArgument(StringUtils.isNotBlank(workingDirName));
            Path workingDirectory = Paths.get(workingDirName);
            java.nio.file.Files.createDirectories(workingDirectory);
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwxrwx---");
            String scriptName = sd.getName() + "-" + sd.getId() + "Cmd.sh";
            Path scriptFilePath = workingDirectory.resolve(scriptName);
            File scriptFile = java.nio.file.Files.createFile(scriptFilePath, PosixFilePermissions.asFileAttribute(perms)).toFile();
            scriptWriter = new ScriptWriter(new BufferedWriter(new FileWriter(scriptFile)));
            writeProcessingCode(externalCode, env, scriptWriter);
            jacsServiceDataPersistence.addServiceEvent(
                    sd,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.CREATED_RUNNING_SCRIPT, String.format("Created the running script for %s: %s", sd.getName(), sd.getArgs()))
            );
            return scriptFile.getAbsolutePath();
        } catch (Exception e) {
            logger.error("Error creating the processing script with {} for {}", externalCode, sd, e);
            jacsServiceDataPersistence.addServiceEvent(
                    sd,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.SCRIPT_CREATION_ERROR, String.format("Error creating the running script for %s: %s", sd.getName(), sd.getArgs()))
            );
            throw new ComputationException(sd, e);
        } finally {
            if (scriptWriter != null) scriptWriter.close();
        }
    }

    private JobTemplate prepareJobTemplate(ExternalCodeBlock externalCode, List<ExternalCodeBlock> externalConfigs, Map<String, String> env, String workingDirName, JacsServiceData serviceContext) throws Exception {

        jacsServiceDataPersistence.updateServiceState(serviceContext, JacsServiceState.RUNNING, Optional.empty());

        JobTemplate jt = new JobTemplate();
        jt.setJobName(serviceContext.getName());
        jt.setArgs(Collections.emptyList());

        File workingDirectory = getJobWorkingDirectory(workingDirName);
        logger.debug("Using working directory {} for {}", workingDirectory, serviceContext);

        jt.setWorkingDir(workingDirectory.getAbsolutePath());
        String configDir = createSubDirectory(jt, "sge_config");
        String errorDir = createSubDirectory(jt, "sge_error");
        String outputDir = createSubDirectory(jt, "sge_output");
        String configFilePattern = serviceContext.getName() + "Configuration.#";

        String processingScript = createProcessingScript(externalCode, env, configDir, serviceContext);
        createConfigFiles(externalConfigs, configDir, configFilePattern, serviceContext);

        jt.setRemoteCommand(processingScript);
        jt.setInputPath(configDir + File.separator + configFilePattern);
        jt.setErrorPath(errorDir + File.separator + serviceContext.getName() + "Error.#");
        jt.setOutputPath(outputDir + File.separator + serviceContext.getName() + "Output.#");
        // Apply a RegEx to replace any non-alphanumeric character with "_".

        String owner = serviceContext.getOwner();
        if (owner==null) {
            owner = ProcessorHelper.getGridBillingAccount(serviceContext.getResources());
        }

        jt.setJobName(owner.replaceAll("\\W", "_") + "_" + serviceContext.getName());
        // Check if the SGE grid requires account info
        // TODO: need to port over ComputeAccounting.getInstance().getComputeAccount
        //setAccount(jt);

        if (StringUtils.isNotBlank(serviceContext.getInputPath())) {
            jt.setInputPath(":" + serviceContext.getInputPath());
        }

        List<String> nativeSpec = createNativeSpec(serviceContext.getResources());
        if (nativeSpec!=null) {
            jt.setNativeSpecification(nativeSpec);
        }

        return jt;
    }

    private List<String> createNativeSpec(Map<String, String> jobResources) {
        List<String> spec = new ArrayList<>();
        // append accountID for billing
        String billingAccount = ProcessorHelper.getGridBillingAccount(jobResources);
        if (StringUtils.isNotBlank(billingAccount)) {
            spec.add("-P "+billingAccount);
        }
        int nProcessingSlots = ProcessorHelper.getProcessingSlots(jobResources);
        StringBuilder resourceBuffer = new StringBuilder();
        //if (nProcessingSlots > 1) {
            // append processing environment
            spec.add("-n "+nProcessingSlots);
            resourceBuffer.append("affinity[core(1)]");
        //}

        long softJobDurationInMins = Math.round((double) ProcessorHelper.getSoftJobDurationLimitInSeconds(jobResources) / 60);
        if (softJobDurationInMins > 0) {
            spec.add("-We "+softJobDurationInMins);
        }

        long hardJobDurationInMins = Math.round((double) ProcessorHelper.getHardJobDurationLimitInSeconds(jobResources) / 60);
        if (hardJobDurationInMins > 0) {
            spec.add("-W "+hardJobDurationInMins);
        }

        String queue = jobResources.get("gridQueue");
        if (StringUtils.isNotBlank(queue)) {
            spec.add("-q "+queue);
        }

        StringBuilder selectResourceBuffer = new StringBuilder();
        String gridNodeArchitecture = ProcessorHelper.getCPUType(jobResources); // sandy, haswell, broadwell, avx2
        if (StringUtils.isNotBlank(gridNodeArchitecture)) {
            selectResourceBuffer.append(gridNodeArchitecture);
        }
        String gridResourceLimits = ProcessorHelper.getGridJobResourceLimits(jobResources);
        if (StringUtils.isNotBlank(gridResourceLimits)) {
            if (selectResourceBuffer.length() > 0) {
                selectResourceBuffer.append(',');
            }
            selectResourceBuffer.append(gridResourceLimits);
        }
        if (selectResourceBuffer.length() > 0) {
            if (resourceBuffer.length() > 0) {
                resourceBuffer.append(' ');
            }
            resourceBuffer
                    .append("select[")
                    .append(selectResourceBuffer)
                    .append(']');
            ;
        }
        if (resourceBuffer.length() > 0) {
            spec.add("-R \""+resourceBuffer+"\"");
        }
        return spec;
    }

    @Override
    protected void writeProcessingCode(ExternalCodeBlock externalCode, Map<String, String> env, ScriptWriter scriptWriter) {
        scriptWriter.add("#!/bin/bash");
        for(String key : env.keySet()) {
            scriptWriter.setVar(key, env.get(key));
        }
        scriptWriter.add(externalCode.toString());
    }

    private File getJobWorkingDirectory(String workingDirName) {
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
        return workingDirectory;
    }

    private String createSubDirectory(JobTemplate jt, String subDir) {
        return FileUtils.createSubDirs(Paths.get(jt.getWorkingDir()), subDir).toString();
    }
}

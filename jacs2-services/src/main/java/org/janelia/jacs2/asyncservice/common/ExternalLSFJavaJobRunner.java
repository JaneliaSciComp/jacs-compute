package org.janelia.jacs2.asyncservice.common;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.apache.commons.lang3.StringUtils;
import org.janelia.cluster.JobFuture;
import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobTemplate;
import org.janelia.jacs2.asyncservice.common.cluster.LsfJavaJobInfo;
import org.janelia.jacs2.asyncservice.common.cluster.MonitoredJobManager;
import org.janelia.jacs2.asyncservice.qualifier.LSFJavaJob;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsJobInstanceInfo;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * External runner which uses the java-lsf library to submit and manage cluster jobs.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@LSFJavaJob
public class ExternalLSFJavaJobRunner extends AbstractExternalProcessRunner {

    private JobManager jobMgr;

    private static final ExecutorService completionMessageExecutor = Executors.newCachedThreadPool((runnable) -> {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            // Ensure that we can shut down without these threads getting in the way
            thread.setName("CompletionMessageThread");
            thread.setDaemon(true);
            return thread;
        });

    @Inject
    public ExternalLSFJavaJobRunner(MonitoredJobManager jobMgr, JacsServiceDataPersistence jacsServiceDataPersistence, Logger logger) {
        super(jacsServiceDataPersistence, logger);
        this.jobMgr = jobMgr.getJobMgr();
    }

    @Override
    public ExeJobInfo runCmds(ExternalCodeBlock externalCode, List<ExternalCodeBlock> externalConfigs, Map<String, String> env, String scriptDirName, String processDirName, JacsServiceData serviceContext) {
        logger.debug("Begin bsub job invocation for {}", serviceContext);
        try {

            JobTemplate jt = prepareJobTemplate(externalCode, externalConfigs, env, processDirName, serviceContext);
            String processingScript = jt.getRemoteCommand();

            int numJobs = externalConfigs.isEmpty() ? 1 : externalConfigs.size();
            final JobFuture future = jobMgr.submitJob(jt, 1, numJobs);

            Long jobId = future.getJobId();

            logger.info("Task was submitted to the cluster as job " + future.getJobId());
            logger.info("Start {} for {} using  env {}", jt.getRemoteCommand(), serviceContext, env);
            logger.info("Submitted job {} for {}", jobId, serviceContext);

            jacsServiceDataPersistence.addServiceEvent(
                    serviceContext,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_SUBMIT, String.format("Submitted job %s {%s} running: %s", serviceContext.getName(), jobId, processingScript))
            );

            LsfJavaJobInfo lsfJavaJobInfo = new LsfJavaJobInfo(jobMgr, jobId, processingScript);

            future.whenCompleteAsync((infos, e) -> {
                processJobCompletion(lsfJavaJobInfo, e);
            }, completionMessageExecutor);

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
            String scriptName = sd.getName() + "Cmd.sh";
            Path scriptFilePath = workingDirectory.resolve(scriptName);
            File scriptFile = java.nio.file.Files.createFile(scriptFilePath, PosixFilePermissions.asFileAttribute(perms)).toFile();
            scriptWriter = new ScriptWriter(new BufferedWriter(new FileWriter(scriptFile)));
            writeProcessingCode(externalCode, env, scriptWriter);
            jacsServiceDataPersistence.addServiceEvent(
                    sd,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.CREATED_RUNNING_SCRIPT, String.format("Created the running script for %s: %s", sd.getName(), sd.getArgs()))
            );
            return scriptFile.getAbsolutePath();
        }
        catch (Exception e) {
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

    protected JobTemplate prepareJobTemplate(ExternalCodeBlock externalCode, List<ExternalCodeBlock> externalConfigs, Map<String, String> env, String workingDirName, JacsServiceData serviceContext) throws Exception {

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

    private void processJobCompletion(LsfJavaJobInfo lsfJavaJobInfo, Throwable e) {
        if (e!=null) {
            logger.error("There was an problem during execution on LSF", e);
        }
        else {
            for (JacsJobInstanceInfo jobInfo : lsfJavaJobInfo.getJobInstanceInfos()) {

                Long queueTimeSeconds = jobInfo.getQueueSecs();
                Long runTimeSeconds = jobInfo.getRunSecs();

                String queueTime = queueTimeSeconds+" sec";
                if (queueTimeSeconds!=null && queueTimeSeconds>300) { // More than 5 minutes, just show the minutes
                    queueTime = TimeUnit.MINUTES.convert(queueTimeSeconds, TimeUnit.SECONDS) + " min";
                }

                String runTime = runTimeSeconds+" sec";
                if (runTimeSeconds!=null && runTimeSeconds>300) {
                    runTime = TimeUnit.MINUTES.convert(runTimeSeconds, TimeUnit.SECONDS) + " min";
                }

                String maxMem = jobInfo.getMaxMem();
                String jobIdStr = jobInfo.getJobId()+"";
                if (jobInfo.getArrayIndex()!=null) {
                    jobIdStr += "."+jobInfo.getArrayIndex();
                }

                logger.info("Job {} was queued for {}, ran for {}, and used "+maxMem+" of memory.", jobIdStr, queueTime, runTime);
                if (jobInfo.getExitCode()!=0) {
                    logger.error("Job {} exited with code {} and reason {}", jobIdStr, jobInfo.getExitCode(), jobInfo.getExitReason());
                }
            }
        }
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
        if (nProcessingSlots > 1) {
            // append processing environment
            spec.add("-n "+nProcessingSlots);
            resourceBuffer.append("affinity[core(1)]");
        }

        long softJobDurationInMins = Math.round(new Double(ProcessorHelper.getSoftJobDurationLimitInSeconds(jobResources)) / 60);
        if (softJobDurationInMins > 0) {
            spec.add("-We "+softJobDurationInMins);
        }

        long hardJobDurationInMins = Math.round(new Double(ProcessorHelper.getHardJobDurationLimitInSeconds(jobResources)) / 60);
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
        String dir = jt.getWorkingDir() + File.separator + subDir;
        File file = new File(dir);
        if (!file.exists()) {
            file.mkdirs();
        }
        return dir;
    }
}

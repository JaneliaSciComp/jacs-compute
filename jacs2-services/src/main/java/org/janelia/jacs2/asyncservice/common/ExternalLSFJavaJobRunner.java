package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobTemplate;
import org.janelia.jacs2.asyncservice.common.cluster.ComputeAccounting;
import org.janelia.jacs2.asyncservice.common.cluster.LsfJavaExeJobHandler;
import org.janelia.jacs2.asyncservice.common.cluster.MonitoredJobManager;
import org.janelia.jacs2.asyncservice.qualifier.LSFJavaJob;
import org.janelia.jacs2.cdi.qualifier.BoolPropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEvent;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * External runner which uses the java-lsf library to submit and manage cluster jobs.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@LSFJavaJob
public class ExternalLSFJavaJobRunner extends AbstractExternalProcessRunner {

    private final JobManager jobMgr;
    private final ComputeAccounting accouting;
    private final boolean requiresAccountInfo;

    @Inject
    public ExternalLSFJavaJobRunner(MonitoredJobManager monitoredJobManager,
                                    JacsServiceDataPersistence jacsServiceDataPersistence,
                                    ComputeAccounting accouting,
                                    @BoolPropertyValue(name = "service.cluster.requiresAccountInfo", defaultValue = true) boolean requiresAccountInfo,
                                    Logger logger) {
        super(jacsServiceDataPersistence, logger);
        this.jobMgr = monitoredJobManager.getJobMgr();
        this.requiresAccountInfo = requiresAccountInfo;
        this.accouting = accouting;
    }

    @Override
    public ExeJobHandler runCmds(ExternalCodeBlock externalCode,
                                 List<ExternalCodeBlock> externalConfigs,
                                 Map<String, String> env,
                                 JacsServiceFolder scriptServiceFolder,
                                 Path processDir,
                                 JacsServiceData serviceContext) {
        logger.debug("Begin bsub job invocation for {}", serviceContext);
        jacsServiceDataPersistence.updateServiceState(serviceContext, JacsServiceState.RUNNING, JacsServiceEvent.NO_EVENT);
        try {
            JobTemplate jt = prepareJobTemplate(externalCode, externalConfigs, env, scriptServiceFolder, processDir, serviceContext);
            String processingScript = jt.getRemoteCommand();

            int numJobs = externalConfigs.isEmpty() ? 1 : externalConfigs.size();
            logger.info("Start {} for {} using  env {}", jt.getRemoteCommand(), serviceContext, env);
            LsfJavaExeJobHandler lsfJobHandler = new LsfJavaExeJobHandler(processingScript, jobMgr, jt, numJobs);

            lsfJobHandler.start();
            logger.info("Submitted job {} for {}", lsfJobHandler.getJobInfo(), serviceContext);

            jacsServiceDataPersistence.addServiceEvent(
                    serviceContext,
                    JacsServiceData.createServiceEvent(
                            JacsServiceEventTypes.CLUSTER_SUBMIT,
                            String.format("Submitted job %s {%s} running: %s", serviceContext.getName(), lsfJobHandler.getJobInfo(), processingScript))
            );

            return lsfJobHandler;
        } catch (Exception e) {
            jacsServiceDataPersistence.updateServiceState(
                    serviceContext,
                    JacsServiceState.ERROR,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_JOB_ERROR, String.format("Error creating DRMAA job %s - %s", serviceContext.getName(), e.getMessage()))
            );
            logger.error("Error creating a cluster job for {}", serviceContext, e);
            throw new ComputationException(serviceContext, e);
        }
    }

    private JobTemplate prepareJobTemplate(ExternalCodeBlock externalCode,
                                           List<ExternalCodeBlock> externalConfigs,
                                           Map<String, String> env,
                                           JacsServiceFolder scriptServiceFolder,
                                           Path processDir,
                                           JacsServiceData serviceContext) throws IOException {

        File jobProcessingDirectory = prepareProcessingDir(processDir);
        logger.debug("Using working directory {} for {}", jobProcessingDirectory, serviceContext);

        String processingScript = createProcessingScript(externalCode, env, scriptServiceFolder, JacsServiceFolder.SERVICE_CONFIG_DIR);
        createConfigFiles(externalConfigs, scriptServiceFolder, JacsServiceFolder.SERVICE_CONFIG_DIR);

        prepareOutputDir(serviceContext.getOutputPath(), "Output directory must be set before running the service " + serviceContext.getName());
        prepareOutputDir(serviceContext.getErrorPath(), "Error file must be set before running the service " + serviceContext.getName());

        JobTemplate jt = new JobTemplate();
        jt.setJobName(serviceContext.getName());
        jt.setArgs(Collections.emptyList());
        jt.setWorkingDir(jobProcessingDirectory.getAbsolutePath());
        jt.setRemoteCommand(processingScript);

        if (CollectionUtils.size(externalConfigs) < 1) {
            jt.setOutputPath(Paths.get(serviceContext.getOutputPath(), scriptServiceFolder.getServiceOutputPattern("")).toString());
            jt.setErrorPath(Paths.get(serviceContext.getErrorPath(), scriptServiceFolder.getServiceErrorPattern("")).toString());
        } else {
            jt.setInputPath(scriptServiceFolder.getServiceFolder(JacsServiceFolder.SERVICE_CONFIG_DIR, scriptServiceFolder.getServiceConfigPattern(".#")).toString());
            jt.setOutputPath(Paths.get(serviceContext.getOutputPath(), scriptServiceFolder.getServiceOutputPattern(".#")).toString());
            jt.setErrorPath(Paths.get(serviceContext.getErrorPath(), scriptServiceFolder.getServiceErrorPattern(".#")).toString());
        }

        // Figure out who is going to get the bill
        String billingAccount = accouting.getComputeAccount(serviceContext);

        // Apply a RegEx to replace any non-alphanumeric character with "_".
        jt.setJobName(billingAccount.replaceAll("\\W", "_") + "_" + serviceContext.getName());

        List<String> nativeSpec = createNativeSpec(serviceContext.getResources());

        if (requiresAccountInfo)
            nativeSpec.add("-P "+billingAccount);

        jt.setNativeSpecification(nativeSpec);

        return jt;
    }

    private List<String> createNativeSpec(Map<String, String> jobResources) {
        List<String> spec = new ArrayList<>();
        int nProcessingSlots = ProcessorHelper.getProcessingSlots(jobResources);
        StringBuilder resourceBuffer = new StringBuilder();
        if (nProcessingSlots > 1) {
            // append processing environment
            spec.add("-n "+nProcessingSlots);
            resourceBuffer.append("affinity[core(1)]");
        }

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

}

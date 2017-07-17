package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.qualifier.LSFPACJob;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.io.File;
import java.util.Map;
import java.util.Optional;

@LSFPACJob
public class ExternalLSFPACJobRunner extends AbstractExternalProcessRunner {

    private final LSFPACHelper lsfPacHelper;

    @Inject
    public ExternalLSFPACJobRunner(JacsServiceDataPersistence jacsServiceDataPersistence, ApplicationConfig applicationConfig, Logger logger) {
        super(jacsServiceDataPersistence, logger);
        lsfPacHelper = new LSFPACHelper(
                applicationConfig.getStringPropertyValue("LSF.PAC.URL"),
                applicationConfig.getStringPropertyValue("LSF.user"),
                applicationConfig.getStringPropertyValue("LSF.password"),
                logger);
    }

    @Override
    public ExeJobInfo runCmds(ExternalCodeBlock externalCode, Map<String, String> env, String workingDirName, JacsServiceData serviceContext) {
        logger.debug("Begin LSF job invocation for {}", serviceContext);
        String processingScript = createProcessingScript(externalCode, env, workingDirName, serviceContext);
        jacsServiceDataPersistence.updateServiceState(serviceContext, JacsServiceState.RUNNING, Optional.empty());
        File outputFile;
        File errorFile;
        try {
            outputFile = prepareOutputFile(serviceContext.getOutputPath(), "Output file must be set before running the service " + serviceContext.getName());
            errorFile = prepareOutputFile(serviceContext.getErrorPath(), "Error file must be set before running the service " + serviceContext.getName());
            String nativeSpec = createNativeSpec(serviceContext.getResources());
            String jobId = lsfPacHelper.submitJob(processingScript, workingDirName, outputFile.getAbsolutePath(), errorFile.getAbsolutePath(), nativeSpec);
            return new LSFPACJobInfo(lsfPacHelper, jobId, processingScript);
        } catch (Exception e) {
            jacsServiceDataPersistence.updateServiceState(
                    serviceContext,
                    JacsServiceState.ERROR,
                    Optional.of(JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_JOB_ERROR, String.format("Error creating DRMAA job %s - %s", serviceContext.getName(), e.getMessage())))
            );
            logger.error("Error creating a DRMAA job {} for {}", processingScript, serviceContext, e);
            throw new ComputationException(serviceContext, e);
        }
    }

    @Override
    protected void writeProcessingCode(ExternalCodeBlock externalCode, Map<String, String> env, ScriptWriter scriptWriter) {
        env.forEach(scriptWriter::exportVar);
        super.writeProcessingCode(externalCode, env, scriptWriter);
    }

    protected String createNativeSpec(Map<String, String> jobResources) {
        StringBuilder nativeSpecBuilder = new StringBuilder();
        // append accountID for billing
        String billingAccount = ProcessorHelper.getGridBillingAccount(jobResources);
        if (StringUtils.isNotBlank(billingAccount)) {
            nativeSpecBuilder.append("-P ").append(billingAccount).append(' ');
        }
        int nProcessingSlots = ProcessorHelper.getProcessingSlots(jobResources);
        if (nProcessingSlots > 1) {
            // append processing environment
            nativeSpecBuilder
                    .append("-n ").append(nProcessingSlots).append(' ')
                    .append("-R ")
                    .append('"')
                    .append("affinity")
                    .append('[')
                    .append("core(1)")
                    .append(']')
                    .append('"')
                    .append(' ')
            ;
        }
        long softJobDurationInMins = ProcessorHelper.getSoftJobDurationLimitInSeconds(jobResources) / 60;
        if (softJobDurationInMins > 0) {
            nativeSpecBuilder.append("-We 0:").append(softJobDurationInMins).append(' ');
        }
        long hardJobDurationInMins = ProcessorHelper.getHardJobDurationLimitInSeconds(jobResources) / 60;
        if (hardJobDurationInMins > 0) {
            nativeSpecBuilder.append("-W 0:").append(hardJobDurationInMins).append(' ');
        }
        if (StringUtils.isNotBlank(jobResources.get("gridQueue"))) {
            nativeSpecBuilder.append("-q ").append(jobResources.get("gridQueue")).append(' ');
        }
        String gridNodeArchitecture = ProcessorHelper.getCPUType(jobResources); // sandy, haswell, broadwell, avx2
        if (StringUtils.isNotBlank(gridNodeArchitecture)) {
            nativeSpecBuilder.append("-R ")
                    .append('"')
                    .append("select")
                    .append('[')
                    .append(gridNodeArchitecture)
                    .append(']')
                    .append('"')
                    .append(' ')
            ;
        }
        String gridResourceLimits = ProcessorHelper.getGridJobResourceLimits(jobResources);
        if (StringUtils.isNotBlank(gridResourceLimits)) {
            nativeSpecBuilder.append("-R ")
                    .append('"')
                    .append(gridResourceLimits)
                    .append('"')
                    .append(' ')
            ;
        }
        return nativeSpecBuilder.toString();
    }

}

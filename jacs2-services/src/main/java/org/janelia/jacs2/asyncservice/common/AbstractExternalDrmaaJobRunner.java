package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEvent;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class AbstractExternalDrmaaJobRunner extends AbstractExternalProcessRunner {

    private final Session drmaaSession;

    AbstractExternalDrmaaJobRunner(Session drmaaSession, JacsServiceDataPersistence jacsServiceDataPersistence, Logger logger) {
        super(jacsServiceDataPersistence, logger);
        this.drmaaSession = drmaaSession;
    }

    @Override
    public ExeJobHandler runCmds(ExternalCodeBlock externalCode,
                                 List<ExternalCodeBlock> externalConfigs,
                                 Map<String, String> env,
                                 JacsServiceFolder scriptServiceFolder,
                                 Path processDir,
                                 JacsServiceData serviceContext) {
        logger.debug("Begin DRMAA job invocation for {}", serviceContext);
        jacsServiceDataPersistence.updateServiceState(serviceContext, JacsServiceState.RUNNING,
                JacsServiceData.createServiceEvent(JacsServiceEventTypes.RUN, "Run service commands via DRMAA"));
        String processingScript = scriptServiceFolder.getServiceFolder("<unknown>").toString();
        JobTemplate jt = null;
        try {
            processingScript = createProcessingScript(externalCode, env, scriptServiceFolder, JacsServiceFolder.SERVICE_CONFIG_DIR);
            List<File> configFiles = createConfigFiles(externalConfigs, scriptServiceFolder, JacsServiceFolder.SERVICE_CONFIG_DIR);

            prepareOutputDir(serviceContext.getOutputPath(), "Output directory must be set before running the service " + serviceContext.getName());
            prepareOutputDir(serviceContext.getErrorPath(), "Error file must be set before running the service " + serviceContext.getName());

            jt = drmaaSession.createJobTemplate();
            File processDirectory = prepareProcessingDir(processDir);
            jt.setWorkingDirectory(processDirectory.getAbsolutePath());
            jt.setJobName(serviceContext.getName());
            jt.setRemoteCommand(processingScript);
            jt.setArgs(Collections.emptyList());
            logger.debug("Using working directory {} for {}", processDirectory, serviceContext);
            jt.setJobEnvironment(env);
            if (CollectionUtils.size(externalConfigs) < 1) {
                jt.setOutputPath(":" + Paths.get(serviceContext.getOutputPath(), scriptServiceFolder.getServiceOutputPattern(".%J")));
                jt.setErrorPath(":" + Paths.get(serviceContext.getErrorPath(), scriptServiceFolder.getServiceErrorPattern(".%J")));
            } else {
                jt.setInputPath(":" + scriptServiceFolder.getServiceFolder(JacsServiceFolder.SERVICE_CONFIG_DIR, scriptServiceFolder.getServiceConfigPattern(".%I")));
                jt.setOutputPath(":" + Paths.get(serviceContext.getOutputPath(), scriptServiceFolder.getServiceOutputPattern(".%J.%I")));
                jt.setErrorPath(":" + Paths.get(serviceContext.getErrorPath(), scriptServiceFolder.getServiceErrorPattern(".%J.%I")));
            }
            String nativeSpec = createNativeSpec(serviceContext.getResources(), processDirectory.getAbsolutePath());
            if (StringUtils.isNotBlank(nativeSpec)) {
                jt.setNativeSpecification(nativeSpec);
            }
            logger.info("Start {} for {} using  env {}", processingScript, serviceContext, env);
            ExeJobHandler jobHandler = new DrmaaExeJobHandler(processingScript, drmaaSession, configFiles.size(), jt);
            String withConfigOption;
            if (configFiles.size() > 0) {
                withConfigOption = " with " + configFiles + " ";
            } else {
                withConfigOption = "";
            }
            jobHandler.start();
            jacsServiceDataPersistence.addServiceEvent(
                    serviceContext,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_SUBMIT,
                            String.format("Submitted job %s {%s} running: %s%s", serviceContext.getName(), jobHandler.getJobInfo(), processingScript, withConfigOption))
            );
            return jobHandler;
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
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.CLUSTER_JOB_ERROR, String.format("Error creating DRMAA job %s - %s", serviceContext.getName(), e.getMessage()))
            );
            logger.error("Error creating a DRMAA job {} for {}", processingScript, serviceContext, e);
            throw new ComputationException(serviceContext, e);
        }
    }

    protected abstract String createNativeSpec(Map<String, String> jobResources, String jobRunningDir);

}

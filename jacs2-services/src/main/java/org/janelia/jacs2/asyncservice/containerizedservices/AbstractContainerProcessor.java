package org.janelia.jacs2.asyncservice.containerizedservices;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.EnumSet;

import javax.enterprise.inject.Instance;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ProcessingLocation;
import org.slf4j.Logger;

abstract class AbstractContainerProcessor<A extends AbstractContainerArgs, R> extends AbstractExeBasedServiceProcessor<R> {

    private final String containerRuntime;

    AbstractContainerProcessor(ServiceComputationFactory computationFactory,
                               JacsServiceDataPersistence jacsServiceDataPersistence,
                               Instance<ExternalProcessRunner> serviceRunners,
                               String defaultWorkingDir,
                               String containerRuntime,
                               JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                               ApplicationConfig applicationConfig,
                               Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.containerRuntime = containerRuntime;
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        A args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        if (createScript(jacsServiceData, args, externalScriptCode.getCodeWriter())) {
            return externalScriptCode;
        } else {
            return null;
        }
    }

    abstract boolean createScript(JacsServiceData jacsServiceData, A args, ScriptWriter scriptWriter);

    abstract A getArgs(JacsServiceData jacsServiceData);

    String getRuntime(AbstractContainerArgs args) {
        // typically for getting executable name we use getFullExecutableName(name) but for container runtime
        // we use the name as it is configured, i.e., docker or singularity or using a fully configured path
        return StringUtils.defaultIfBlank(args.runtime, containerRuntime);
    }

    String serviceScratchDir(JacsServiceData jacsServiceData) {
        String scratchDir = getApplicationConfig().getStringPropertyValue("service.DefaultScratchDir", "/scratch");
        if (EnumSet.of(ProcessingLocation.LSF_JAVA).contains(jacsServiceData.getProcessingLocation())
                || Files.exists(Paths.get(scratchDir))) {
            return scratchDir;
        } else {
            return null;
        }
    }

}

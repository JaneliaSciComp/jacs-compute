package org.janelia.jacs2.asyncservice.containerizedservices;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

import javax.enterprise.inject.Instance;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

abstract class AbstractSingularityContainerProcessor<R> extends AbstractExeBasedServiceProcessor<R> {

    private final String singularityExecutable;
    private final String localSingularityImagesPath;

    AbstractSingularityContainerProcessor(ServiceComputationFactory computationFactory,
                                          JacsServiceDataPersistence jacsServiceDataPersistence,
                                          Instance<ExternalProcessRunner> serviceRunners,
                                          String defaultWorkingDir,
                                          String singularityExecutable,
                                          String localSingularityImagesPath,
                                          JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                                          ApplicationConfig applicationConfig,
                                          Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.singularityExecutable = singularityExecutable;
        this.localSingularityImagesPath = localSingularityImagesPath;
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        AbstractSingularityContainerArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        createScript(args, externalScriptCode.getCodeWriter());
        return externalScriptCode;
    }

    abstract void createScript(AbstractSingularityContainerArgs args, ScriptWriter scriptWriter);

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        AbstractSingularityContainerArgs args = getArgs(jacsServiceData);
        if (args.noHttps()) {
            return ImmutableMap.of("SINGULARITY_NOHTTPS", "True");
        } else {
            return ImmutableMap.of();
        }
    }

    abstract AbstractSingularityContainerArgs getArgs(JacsServiceData jacsServiceData);

    Optional<String> getLocalImagesDir(AbstractSingularityContainerArgs args) {
        return SingularityContainerHelper.getLocalImagesDir(args, localSingularityImagesPath);
    }

    ContainerImage getLocalContainerImage(AbstractSingularityContainerArgs args) {
        return SingularityContainerHelper.getLocalContainerImageMapper().apply(args, localSingularityImagesPath);
    }

    String getRuntime(AbstractSingularityContainerArgs args) {
        return SingularityContainerHelper.getRuntime(args).orElseGet(() -> getFullExecutableName(singularityExecutable));
    }
}

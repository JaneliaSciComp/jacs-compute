package org.janelia.jacs2.asyncservice.containerizedservices;

import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.nio.file.Path;

@Named("pullSingularityContainer")
public class PullSingularityContainerProcessor extends AbstractSingularityContainerProcessor<PullSingularityContainerProcessor.PullSingularityContainerArgs, File> {

    static class PullSingularityContainerArgs extends AbstractSingularityContainerArgs {
        PullSingularityContainerArgs() {
            super("Service that pulls a singularity container image");
        }
    }

    @Inject
    PullSingularityContainerProcessor(ServiceComputationFactory computationFactory,
                                      JacsServiceDataPersistence jacsServiceDataPersistence,
                                      @Any Instance<ExternalProcessRunner> serviceRunners,
                                      @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                      @PropertyValue(name = "Singularity.Bin.Path") String singularityExecutable,
                                      @PropertyValue(name = "Singularity.LocalImages.Path") String localSingularityImagesPath,
                                      JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                                      @ApplicationProperties ApplicationConfig applicationConfig,
                                      Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, singularityExecutable, localSingularityImagesPath, jacsJobInstanceInfoDao, applicationConfig, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(PullSingularityContainerProcessor.class, createContainerArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                PullSingularityContainerArgs args = getArgs(depResults.getJacsServiceData());
                return getLocalContainerImage(args).toFile().exists();
            }

            @Override
            public File collectResult(JacsServiceResult<?> depResults) {
                PullSingularityContainerArgs args = getArgs(depResults.getJacsServiceData());
                return getLocalContainerImage(args).toFile();
            }
        };
    }


    protected Path getProcessDir(JacsServiceData jacsServiceData) {
        PullSingularityContainerArgs args = getArgs(jacsServiceData);
        return getLocalImagesDir(args);
    }

    @Override
    void createScript(PullSingularityContainerArgs args, ScriptWriter scriptWriter) {
        scriptWriter
                .addWithArgs(getRuntime((args)))
                .addArg("pull")
                .addArgs("--name", getLocalContainerImage(args).getFileName().toString())
                .endArgs();
    }

    @Override
    public PullSingularityContainerArgs createContainerArgs() {
        return new PullSingularityContainerArgs();
    }

}

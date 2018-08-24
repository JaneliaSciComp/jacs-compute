package org.janelia.jacs2.asyncservice.containerizedservices;

import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
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
import java.nio.file.Paths;

@Named("pullSingularityContainer")
public class PullSingularityContainerProcessor extends AbstractSingularityContainerProcessor<File> {

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
        return ServiceArgs.getMetadata(PullSingularityContainerProcessor.class, new PullSingularityContainerArgs());
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

    @Override
    protected ServiceComputation<JacsServiceResult<Void>> processing(JacsServiceResult<Void> depResults) {
        PullSingularityContainerArgs args = getArgs(depResults.getJacsServiceData());
        Path localContainerImage = getLocalContainerImage(args);
        if (localContainerImage.toFile().exists()) {
            return computationFactory.newCompletedComputation(depResults);
        } else {
            return super.processing(depResults);
        }
    }

    @Override
    protected Path getProcessDir(JacsServiceData jacsServiceData) {
        PullSingularityContainerArgs args = getArgs(jacsServiceData);
        return getLocalImagesDir(args).map(p -> Paths.get(p)).orElseGet(() -> super.getProcessDir(jacsServiceData));
    }

    @Override
    void createScript(AbstractSingularityContainerArgs args, ScriptWriter scriptWriter) {
        scriptWriter
                .addWithArgs(getRuntime((args)))
                .addArg("pull")
                .addArgs("--name", getLocalContainerImage(args).getFileName().toString())
                .addArgs(args.containerLocation)
                .endArgs();
    }

    PullSingularityContainerArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new PullSingularityContainerArgs());
    }
}

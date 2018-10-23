package org.janelia.jacs2.asyncservice.containerizedservices;

import com.fasterxml.jackson.core.type.TypeReference;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.utils.HttpUtils;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

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
                return getLocalContainerImage(args).localImageExists();
            }

            @Override
            public File collectResult(JacsServiceResult<?> depResults) {
                PullSingularityContainerArgs args = getArgs(depResults.getJacsServiceData());
                return getLocalContainerImage(args).getLocalImagePath().toFile();
            }
        };
    }

    @Override
    protected ServiceComputation<JacsServiceResult<Void>> processing(JacsServiceResult<Void> depResults) {
        PullSingularityContainerArgs args = getArgs(depResults.getJacsServiceData());
        ContainerImage localContainerImage = getLocalContainerImage(args);
        if (localContainerImage.localImageExists()) {
            return computationFactory.newCompletedComputation(depResults);
        } else if (localContainerImage.requiresPull()) {
            return super.processing(depResults);
        } else {
            return computationFactory.newCompletedComputation(localContainerImage.localPath.resolve(localContainerImage.imageName + ".tmp"))
                    .thenApply(tempImagePath -> writeLocalImage(args, tempImagePath))
                    .thenApply(tempImagePath -> {
                        try {
                            // this should copy to the same filesystem
                            Files.move(tempImagePath, localContainerImage.getLocalImagePath());
                            return new JacsServiceResult<>(depResults.getJacsServiceData());
                        } catch (IOException e) {
                            logger.error("Error renaming temporary image {} to {}",
                                    tempImagePath, localContainerImage.getLocalImagePath(), e);
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }

    private Path writeLocalImage(AbstractSingularityContainerArgs args, Path imagePath) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(args.containerLocation);
            Response response = target.request().get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                logger.error("Request for json config to {} returned with {}", target, response.getStatus());
                throw new IllegalStateException(args.containerLocation + " returned with " + response.getStatus());
            }
            Files.copy(response.readEntity(InputStream.class), imagePath);
            return imagePath;
        } catch (IllegalStateException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpclient.close();
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
                .addArgs("--name", getLocalContainerImage(args).imageName)
                .addArgs(args.containerLocation)
                .endArgs();
    }

    PullSingularityContainerArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new PullSingularityContainerArgs());
    }
}

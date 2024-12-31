package org.janelia.jacs2.asyncservice.containerizedservices;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
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
import org.janelia.jacs2.utils.HttpUtils;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Named("pullSingularityContainer")
public class PullSingularityContainerProcessor extends AbstractContainerProcessor<PullSingularityContainerArgs, File> {

    private final String localSingularityImagesPath;

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
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, singularityExecutable, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.localSingularityImagesPath = localSingularityImagesPath;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(PullSingularityContainerProcessor.class, new PullSingularityContainerArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                PullSingularityContainerArgs args = getArgs(jacsServiceData);
                return getLocalContainerImage(args).localImageExists();
            }

            @Override
            public File collectResult(JacsServiceData jacsServiceData) {
                PullSingularityContainerArgs args = getArgs(jacsServiceData);
                return getLocalContainerImage(args).getLocalImagePath().toFile();
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<File>> process(JacsServiceData jacsServiceData) {
        PullSingularityContainerArgs args = getArgs(jacsServiceData);
        ContainerImage localContainerImage = getLocalContainerImage(args);
        if (localContainerImage.localImageExists()) {
            return computationFactory.newCompletedComputation(updateServiceResult(jacsServiceData, getResultHandler().collectResult(jacsServiceData)));
        } else if (localContainerImage.requiresPull()) {
            return super.process(jacsServiceData);
        } else {
            return computationFactory.newCompletedComputation(localContainerImage.localPath.resolve(localContainerImage.imageName + ".tmp"))
                    .thenApply(tempImagePath -> writeLocalImage(args, tempImagePath))
                    .thenApply(tempImagePath -> {
                        try {
                            // this should copy to the same filesystem
                            Files.move(tempImagePath, localContainerImage.getLocalImagePath());
                            return updateServiceResult(jacsServiceData, getResultHandler().collectResult(jacsServiceData));
                        } catch (IOException e) {
                            logger.error("Error renaming temporary image {} to {}",
                                    tempImagePath, localContainerImage.getLocalImagePath(), e);
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }

    private Path writeLocalImage(AbstractContainerArgs args, Path imagePath) {
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
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        PullSingularityContainerArgs args = getArgs(jacsServiceData);
        if (args.noHttps()) {
            return ImmutableMap.of("SINGULARITY_NOHTTPS", "True");
        } else {
            return ImmutableMap.of();
        }
    }

    @Override
    protected Path getProcessDir(JacsServiceData jacsServiceData) {
        PullSingularityContainerArgs args = getArgs(jacsServiceData);
        return getLocalImagesDir(args).map(p -> Paths.get(p)).orElseGet(() -> super.getProcessDir(jacsServiceData));
    }

    private Optional<String> getLocalImagesDir(PullSingularityContainerArgs args) {
        return SingularityContainerHelper.getLocalImagesDir(args, localSingularityImagesPath);
    }

    @Override
    boolean createScript(JacsServiceData jacsServiceData, PullSingularityContainerArgs args, ScriptWriter scriptWriter) {
        scriptWriter
                .addWithArgs(getRuntime((args)))
                .addArg("pull")
                .addArgs("--name", getLocalContainerImage(args).imageName)
                .addArgs(args.containerLocation)
                .endArgs();
        return true;
    }

    PullSingularityContainerArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new PullSingularityContainerArgs());
    }

    private ContainerImage getLocalContainerImage(PullSingularityContainerArgs args) {
        return SingularityContainerHelper.getLocalContainerImageMapper().apply(args, localSingularityImagesPath);
    }

}

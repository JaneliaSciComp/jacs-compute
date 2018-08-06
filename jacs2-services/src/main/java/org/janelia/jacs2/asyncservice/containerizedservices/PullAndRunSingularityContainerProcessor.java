package org.janelia.jacs2.asyncservice.containerizedservices;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

@Named("runSingularityContainer")
public class PullAndRunSingularityContainerProcessor extends AbstractServiceProcessor<Void> {

    private final String localSingularityImagesPath;
    private final WrappedServiceProcessor<PullSingularityContainerProcessor, File> pullContainerProcessor;
    private final WrappedServiceProcessor<SimpleRunSingularityContainerProcessor, Void> runContainerProcessor;

    @Inject
    PullAndRunSingularityContainerProcessor(ServiceComputationFactory computationFactory,
                                            JacsServiceDataPersistence jacsServiceDataPersistence,
                                            @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                            @PropertyValue(name = "Singularity.LocalImages.Path") String localSingularityImagesPath,
                                            PullSingularityContainerProcessor pullContainerProcessor,
                                            SimpleRunSingularityContainerProcessor runContainerProcessor,
                                            Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.localSingularityImagesPath = localSingularityImagesPath;
        this.pullContainerProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, pullContainerProcessor);
        this.runContainerProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, runContainerProcessor);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(PullAndRunSingularityContainerProcessor.class, new RunSingularityContainerArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        try {
            RunSingularityContainerArgs args = getArgs(jacsServiceData);
            // extract data mount points
            Map<String, String> dataMountPoints = new LinkedHashMap<>();
            // !!!! TODO - dataMountPoints need to be populated
            String containerLocation = getContainerLocation(args);
            return pullContainerProcessor.process(
                    new ServiceExecutionContext.Builder(jacsServiceData)
                            .description("Pull container image " + args.containerLocation)
                            .build(),
                    new ServiceArg("-containerLocation", containerLocation))
                    .thenCompose(containerImageResult -> {
                        String bindPath = dataMountPoints.entrySet().stream()
                                .map(en -> {
                                    if (StringUtils.isBlank(en.getValue())) {
                                        return en.getKey();
                                    } else {
                                        return en.getKey() + ":" + en.getValue();
                                    }
                                })
                                .reduce((b1, b2) -> b1 + "," + b2)
                                .orElse("");
                        return runContainerProcessor.process(
                                new ServiceExecutionContext.Builder(jacsServiceData)
                                        .description("Run " + containerImageResult.getResult().getAbsolutePath())
                                        .waitFor(containerImageResult.getJacsServiceData())
                                        .addResources(jacsServiceData.getResources())
                                        .build(),
                                new ServiceArg("-containerLocation", containerImageResult.getResult().getAbsolutePath()),
                                new ServiceArg("-bindPaths", bindPath),
                                new ServiceArg("-appArgs", args.appArgs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(""))
                        );
                    })
                    ;
        } catch (Exception e) {
            logger.warn("Failed to read step config for {}", jacsServiceData, e);
            return computationFactory.newFailedComputation(new ComputationException(jacsServiceData, e));
        }
    }

    private RunSingularityContainerArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new RunSingularityContainerArgs());
    }

    private String getContainerLocation(RunSingularityContainerArgs args) {
        return SingularityContainerHelper.getLocalContainerImageMapper().andThen(Path::toString).apply(args, this.localSingularityImagesPath);
    }

}

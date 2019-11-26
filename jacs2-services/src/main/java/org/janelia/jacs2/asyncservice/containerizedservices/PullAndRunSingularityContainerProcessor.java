package org.janelia.jacs2.asyncservice.containerizedservices;

import java.io.File;

import javax.inject.Inject;
import javax.inject.Named;

import com.google.common.collect.ImmutableSet;

import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
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

@Named("runSingularityContainer")
public class PullAndRunSingularityContainerProcessor extends AbstractServiceProcessor<Void> {

    private final WrappedServiceProcessor<PullSingularityContainerProcessor, File> pullContainerProcessor;
    private final WrappedServiceProcessor<SimpleRunSingularityContainerProcessor, Void> runContainerProcessor;

    @Inject
    PullAndRunSingularityContainerProcessor(ServiceComputationFactory computationFactory,
                                            JacsServiceDataPersistence jacsServiceDataPersistence,
                                            @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                            PullSingularityContainerProcessor pullContainerProcessor,
                                            SimpleRunSingularityContainerProcessor runContainerProcessor,
                                            Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.pullContainerProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, pullContainerProcessor);
        this.runContainerProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, runContainerProcessor);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(PullAndRunSingularityContainerProcessor.class, new SingularityContainerArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        SingularityContainerArgs args = getArgs(jacsServiceData);
        return pullContainerProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Pull container image " + args.containerLocation)
                        .build(),
                new ServiceArg("-containerLocation", args.containerLocation),
                new ServiceArg("-runtime", args.runtime),
                new ServiceArg("-enableHttps", args.enableHttps),
                new ServiceArg("-containerImagesDir", args.containerImagesDirectory))
                .thenCompose(containerImageResult -> runContainerProcessor.process(
                        new ServiceExecutionContext.Builder(jacsServiceData)
                                .description("Run " + containerImageResult.getResult().getAbsolutePath())
                                .waitFor(containerImageResult.getJacsServiceData())
                                .addResources(jacsServiceData.getResources())
                                .build(),
                        new ServiceArg("-containerLocation", containerImageResult.getResult().getAbsolutePath()),
                        new ServiceArg("-runtime", args.runtime),
                        new ServiceArg("-appName", args.appName),
                        new ServiceArg("-bindPaths", args.bindPathsAsString(ImmutableSet.copyOf(args.bindPaths))),
                        new ServiceArg("-overlay", args.overlay),
                        new ServiceArg("-enableNV", args.enableNV),
                        new ServiceArg("-initialPwd", args.initialPwd),
                        new ServiceArg("-appArgs", args.appArgs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse("")),
                        new ServiceArg("-batchJobArgs", args.batchJobArgs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse("")),
                        new ServiceArg("", args.getRemainingArgs().stream().reduce((s1, s2) -> s1 + "," + s2).orElse(""))
                        ))
                .thenApply(r -> new JacsServiceResult<>(jacsServiceData))
                ;
    }

    private SingularityContainerArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SingularityContainerArgs());
    }

}

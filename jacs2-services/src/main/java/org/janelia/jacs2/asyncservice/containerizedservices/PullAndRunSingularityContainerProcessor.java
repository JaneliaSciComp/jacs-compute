package org.janelia.jacs2.asyncservice.containerizedservices;

import java.io.File;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.beust.jcommander.Parameter;
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

@Dependent
@Named("runSingularityContainer")
public class PullAndRunSingularityContainerProcessor extends AbstractServiceProcessor<Void> {

    private static class SingularityContainerArgs extends SingularityRunContainerArgs {
        @Parameter(names = "-enableHttps", description = "Enable HTTPS for retrieving the container image")
        boolean enableHttps;
        @Parameter(names = "-containerImagesDir", description = "Local container images directory")
        String containerImagesDirectory;

        SingularityContainerArgs() {
            this("Service that pulls and runs a singularity container");
        }

        SingularityContainerArgs(String description) {
            super(description);
        }
    }

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
                        new ServiceArg("-expandDir", args.expandedDir),
                        new ServiceArg("-expandDepth", args.expandedDepth),
                        new ServiceArg("-expandPattern", args.expandedPattern),
                        new ServiceArg("-expandedArgFlag", args.expandedArgFlag),
                        new ServiceArg("-expandedArgList", args.expandedArgList.stream().reduce((s1, s2) -> s1 + "," + s2).orElse("")),
                        new ServiceArg("-cancelIfEmptyExpansion", args.cancelIfEmptyExpansion),
                        new ServiceArg("-runtimeArgs", args.runtimeArgs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse("")),
                        new ServiceArg("-appArgs", args.appArgs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse("")),
                        new ServiceArg("-batchJobArgs", args.batchJobArgs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse("")),
                        new ServiceArg(args.getRemainingArgs())
                        ))
                .thenApply(r -> new JacsServiceResult<>(jacsServiceData))
                ;
    }

    private SingularityContainerArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SingularityContainerArgs());
    }

}

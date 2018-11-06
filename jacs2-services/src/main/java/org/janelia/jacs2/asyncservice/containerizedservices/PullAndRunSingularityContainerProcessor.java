package org.janelia.jacs2.asyncservice.containerizedservices;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        return ServiceArgs.getMetadata(PullAndRunSingularityContainerProcessor.class, new ExpandedRunSingularityContainerArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        ExpandedRunSingularityContainerArgs args = getArgs(jacsServiceData);
        return pullContainerProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Pull container image " + args.containerLocation)
                        .build(),
                new ServiceArg("-containerLocation", args.containerLocation),
                new ServiceArg("-singularityRuntime", args.singularityRuntime),
                new ServiceArg("-enableHttps", args.enableHttps),
                new ServiceArg("-containerImagesDir", args.containerImagesDirectory),
                new ServiceArg("-containerName", args.containerName))
                .thenCompose(containerImageResult -> createExpandedItemsComputation(containerImageResult, args, expandArguments(args), jacsServiceData))
                ;
    }

    private ExpandedRunSingularityContainerArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new ExpandedRunSingularityContainerArgs());
    }

    private List<String> expandArguments(ExpandedRunSingularityContainerArgs args) {
        Stream<String> expandedArgsStream;
        if (StringUtils.isNotBlank(args.expandedDir)) {
            expandedArgsStream = FileUtils.lookupFiles(Paths.get(args.expandedDir), args.expandedDepth, args.expandedPattern)
                    .filter(p -> Files.isRegularFile(p))
                    .map(Path::toString);
        } else {
            expandedArgsStream = Stream.of();
        }
        return Stream.concat(expandedArgsStream, args.expandedArgList.stream())
                .collect(Collectors.toList());
    }

    private ServiceComputation<JacsServiceResult<Void>> createExpandedItemsComputation(JacsServiceResult<File> containerImageResult,
                                                                                       ExpandedRunSingularityContainerArgs args,
                                                                                       List<String> expandedArgs,
                                                                                       JacsServiceData jacsServiceData) {
        if (expandedArgs.isEmpty()) {
            if (args.cancelIfEmptyExpansion)
                return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData));
            else
                return runContainerProcessor.process(
                        new ServiceExecutionContext.Builder(jacsServiceData)
                                .description("Run " + containerImageResult.getResult().getAbsolutePath())
                                .waitFor(containerImageResult.getJacsServiceData())
                                .addResources(jacsServiceData.getResources())
                                .build(),
                        new ServiceArg("-containerLocation", containerImageResult.getResult().getAbsolutePath()),
                        new ServiceArg("-singularityRuntime", args.singularityRuntime),
                        new ServiceArg("-enableHttps", args.enableHttps),
                        new ServiceArg("-op", args.operation.toString()),
                        new ServiceArg("-appName", args.appName),
                        new ServiceArg("-bindPaths", args.bindPathsAsString()),
                        new ServiceArg("-overlay", args.overlay),
                        new ServiceArg("-enableNV", args.enableNV),
                        new ServiceArg("-initialPwd", args.initialPwd),
                        new ServiceArg("-appArgs", args.appArgs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse("")),
                        new ServiceArg("", args.getRemainingArgs().stream().reduce((s1, s2) -> s1 + "," + s2).orElse(""))
                );
        } else {
            List<ServiceComputation<?>> expandedItemJobs = expandedArgs.stream()
                    .map(expandedArg -> {
                        ServiceExecutionContext serviceExecutionContext = new ServiceExecutionContext.Builder(jacsServiceData)
                                .description("Run " + containerImageResult.getResult().getAbsolutePath() + " for " + expandedArg)
                                .waitFor(containerImageResult.getJacsServiceData())
                                .addResources(jacsServiceData.getResources())
                                .build();
                        List<ServiceArg> serviceArgs = ImmutableList.<ServiceArg>builder()
                                .add(new ServiceArg("-containerLocation", containerImageResult.getResult().getAbsolutePath()))
                                .add(new ServiceArg("-singularityRuntime", args.singularityRuntime))
                                .add(new ServiceArg("-enableHttps", args.enableHttps))
                                .add(new ServiceArg("-op", args.operation.toString()))
                                .add(new ServiceArg("-appName", args.appName))
                                .add(new ServiceArg("-bindPaths", args.bindPathsAsString()))
                                .add(new ServiceArg("-overlay", args.overlay))
                                .add(new ServiceArg("-enableNV", args.enableNV))
                                .add(new ServiceArg("-initialPwd", args.initialPwd))
                                .add(new ServiceArg("-appArgs", args.appArgs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse("")))
                                .addAll(args.hasExpandedArgFlag()
                                    ? ImmutableList.of(
                                            new ServiceArg(args.expandedArgFlag, expandedArg),
                                            new ServiceArg("", args.getRemainingArgs())
                                        )
                                    : ImmutableList.of(
                                            new ServiceArg("", Stream.concat(
                                                    Stream.of(expandedArg),
                                                    args.getRemainingArgs().stream()).collect(Collectors.toList()))
                                        )
                                )
                                .build();
                        return runContainerProcessor.process(serviceExecutionContext, serviceArgs);
                    })
                    .collect(Collectors.toList());
            return computationFactory.newCompletedComputation(containerImageResult)
                    .thenCombineAll(expandedItemJobs, (ignored, expendedItemResults) -> new JacsServiceResult<>(jacsServiceData));
        }
    }

}

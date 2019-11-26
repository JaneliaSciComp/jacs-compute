package org.janelia.jacs2.asyncservice.containerizedservices;

import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.DelegateServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Named("runContainer")
public class RunContainerProcessor extends AbstractServiceProcessor<Void> {

    private static final String DOCKER_PROCESSOR = "docker";
    private static final String SINGULARITY_PROCESSOR = "singularity";

    private static class ContainerArgs extends RunContainerArgs {
        @Parameter(names = "-containerProcessor", description = "Which container processor to use - docker or singularity")
        String containerProcessor = "singularity";
        @Parameter(names = "-enableHttps", description = "Enable HTTPS for retrieving the container image")
        boolean enableHttps;
        @Parameter(names = "-containerImagesDir", description = "Local container images directory")
        String containerImagesDirectory;
        @Parameter(names = "-overlay", description = "Container overlay")
        String overlay;
        @Parameter(names = "-enableNV", description = "Enable NVidia support")
        boolean enableNV;
        @Parameter(names = "-initialPwd", description = "Initial working directory inside the container")
        String initialPwd;

        ContainerArgs() {
            this("Service that pulls and runs a docker or a singularity container");
        }

        ContainerArgs(String description) {
            super(description);
        }
    }

    private final DelegateServiceProcessor<PullAndRunSingularityContainerProcessor, Void> singularityContainerProcessor;
    private final DelegateServiceProcessor<SimpleRunDockerContainerProcessor, Void> dockerContainerProcessor;

    @Inject
    RunContainerProcessor(ServiceComputationFactory computationFactory,
                          JacsServiceDataPersistence jacsServiceDataPersistence,
                          @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                          PullAndRunSingularityContainerProcessor singularityContainerProcessor,
                          SimpleRunDockerContainerProcessor dockerContainerProcessor,
                          Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.singularityContainerProcessor = new DelegateServiceProcessor<>(singularityContainerProcessor, jacsServiceData -> {
            ContainerArgs args = getArgs(jacsServiceData);
            return ImmutableList.<ServiceArg>builder()
                    .add(new ServiceArg("-containerImagesDir", args.containerImagesDirectory))
                    .add(new ServiceArg("-enableHttps", args.enableHttps))
                    .add(new ServiceArg("-overlay", args.overlay))
                    .add(new ServiceArg("-enableNV", args.enableNV))
                    .add(new ServiceArg("-initialPwd", args.initialPwd))
                    .addAll(mapCommonArgs(args))
                    .build();
        });
        this.dockerContainerProcessor = new DelegateServiceProcessor<>(dockerContainerProcessor, jacsServiceData -> {
            ContainerArgs args = getArgs(jacsServiceData);
            return mapCommonArgs(args);
        });
    }

    private List<ServiceArg> mapCommonArgs(ContainerArgs args) {
        return ImmutableList.of(
                new ServiceArg("-containerLocation", args.containerLocation),
                new ServiceArg("-runtime", args.runtime),
                new ServiceArg("-appName", args.appName),
                new ServiceArg("-bindPaths", args.bindPathsAsString(args.bindPaths)),
                new ServiceArg("-expandDir", args.expandedDir),
                new ServiceArg("-expandDepth", args.expandedDepth),
                new ServiceArg("-expandPattern", args.expandedPattern),
                new ServiceArg("-expandedArgFlag", args.expandedArgFlag),
                new ServiceArg("-expandedArgList", args.expandedArgList.stream().reduce((s1, s2) -> s1 + "," + s2).orElse("")),
                new ServiceArg("-cancelIfEmptyExpansion", args.cancelIfEmptyExpansion),
                new ServiceArg("-appArgs", args.appArgs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse("")),
                new ServiceArg("-batchJobArgs", args.batchJobArgs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse("")),
                new ServiceArg(args.getRemainingArgs())
            );
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(RunContainerProcessor.class, new ContainerArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        ContainerArgs args = getArgs(jacsServiceData);
        ServiceProcessor<Void> containerProcessor = getContainerProcessor(args);
        return containerProcessor.process(jacsServiceData)
                .thenApply(r -> new JacsServiceResult<>(jacsServiceData))
                ;
    }

    private ContainerArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new ContainerArgs());
    }

    private ServiceProcessor<Void> getContainerProcessor(ContainerArgs args) {
        String containerProcessor = StringUtils.lowerCase(StringUtils.defaultIfBlank(args.containerProcessor, SINGULARITY_PROCESSOR));
        switch (containerProcessor) {
            case DOCKER_PROCESSOR: return dockerContainerProcessor;
            default: return singularityContainerProcessor;
        }
    }
}

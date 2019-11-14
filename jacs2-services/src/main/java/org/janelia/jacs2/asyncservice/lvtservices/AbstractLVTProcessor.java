package org.janelia.jacs2.asyncservice.lvtservices;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.containerizedservices.PullAndRunSingularityContainerProcessor;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

abstract class AbstractLVTProcessor<A extends LVTArgs, R> extends AbstractServiceProcessor<R> {

    private final WrappedServiceProcessor<PullAndRunSingularityContainerProcessor, Void> pullAndRunContainerProcessor;
    private final String defaultContainerImage;

    AbstractLVTProcessor(ServiceComputationFactory computationFactory,
                         JacsServiceDataPersistence jacsServiceDataPersistence,
                         String defaultWorkingDir,
                         PullAndRunSingularityContainerProcessor pullAndRunContainerProcessor,
                         String defaultContainerImage,
                         Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.pullAndRunContainerProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, pullAndRunContainerProcessor);
        this.defaultContainerImage = defaultContainerImage;
    }

    @Override
    public ServiceComputation<JacsServiceResult<R>> process(JacsServiceData jacsServiceData) {
        LVTArgs args = getArgs(jacsServiceData);
        String containerLocation = StringUtils.defaultIfBlank(args.toolContainerImage, defaultContainerImage);
        if (StringUtils.isBlank(containerLocation)) {
            throw new IllegalArgumentException("No tool container has been configured or specified in the service arguments");
        }
        return invokeLVTool(jacsServiceData)
                .thenApply(sr -> updateServiceResult(jacsServiceData, collectResult(jacsServiceData)))
                ;
    }

    ServiceComputation<JacsServiceResult<Void>> invokeLVTool(JacsServiceData jacsServiceData) {
        A args = getArgs(jacsServiceData);
        String containerLocation = StringUtils.defaultIfBlank(args.toolContainerImage, defaultContainerImage);
        if (StringUtils.isBlank(containerLocation)) {
            throw new IllegalArgumentException("No tool container has been configured or specified in the service arguments");
        }
        return pullAndRunContainerProcessor.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Pull container image " + containerLocation)
                        .build(),
                new ServiceArg("-containerLocation", containerLocation),
                new ServiceArg("-appArgs", getAppArgs(args)),
                new ServiceArg("-bindPaths", args.inputDir),
                new ServiceArg("-bindPaths", args.outputDir + ":" + args.outputDir + ":rw"),
                new ServiceArg("-batchJobArgs", getAppBatchArgs(args))
        ).thenApply(containerRunResult -> new JacsServiceResult<>(jacsServiceData));
    }

    A getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), createToolArgs());
    }

    abstract A createToolArgs();

    abstract String getAppArgs(A args);

    String getAppBatchArgs(A args) {
        return null;
    }

    abstract R collectResult(JacsServiceData jacsServiceData);
}

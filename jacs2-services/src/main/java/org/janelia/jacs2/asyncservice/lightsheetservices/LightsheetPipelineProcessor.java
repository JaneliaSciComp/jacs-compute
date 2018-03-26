package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Map;

/**
 * Complete lightsheet processing service which invokes multiple LightsheetPipelineProcessor steps.
 *
 * @author David Ackerman
 */
@Named("lightsheetProcessing")
public class LightsheetPipelineProcessor extends AbstractServiceProcessor<Void> {

    static class LightsheetProcessingArgs extends ServiceArgs {
        @Parameter(names = "-configAddress", description = "Address for accessing job's config json.")
        String configAddress;
        @Parameter(names = "-allSelectedStepNames", description = "Selected pipeline steps to run", required = true)
        List<LightsheetPipelineStep> allSelectedSteps;
        @Parameter(names = "-allSelectedTimePoints", description = "Selected time points - the number of selected timepoints must be equal to the number of steps", required = true)
        List<String> allSelectedTimePoints;
    }

    private final WrappedServiceProcessor<LightsheetPipelineStepProcessor, Void> lightsheetPipelineStepProcessor;

    @Inject
    LightsheetPipelineProcessor(ServiceComputationFactory computationFactory,
                                JacsServiceDataPersistence jacsServiceDataPersistence,
                                @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                LightsheetPipelineStepProcessor lightsheetPipelineStepProcessor,
                                Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.lightsheetPipelineStepProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, lightsheetPipelineStepProcessor);

    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LightsheetPipelineProcessor.class, new LightsheetProcessingArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        LightsheetProcessingArgs args = getArgs(jacsServiceData);
        final Integer timePointsPerJob = 1; // FIXED AT 4

        ServiceComputation<JacsServiceResult<Void>> stage = lightsheetPipelineStepProcessor.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Step 1: " + args.allSelectedSteps.get(0))
                        .addDictionaryArgs((Map<String, Object>) jacsServiceData.getDictionaryArgs().get(args.allSelectedSteps.get(0).name()))
                        .build(),
                new ServiceArg("-step", args.allSelectedSteps.get(0).name()),
                new ServiceArg("-numTimePoints", args.allSelectedTimePoints.get(0)),
                new ServiceArg("-timePointsPerJob", timePointsPerJob.toString()),
                new ServiceArg("-configAddress", args.configAddress)
        );
        int nSteps = args.allSelectedSteps.size();
        for (int i = 1; i < nSteps; i++) {
            final int stepIndex = i;
            stage = stage.thenCompose(firstStageResult -> {
                // firstStageResult.getResult
                return lightsheetPipelineStepProcessor.process(
                        new ServiceExecutionContext.Builder(jacsServiceData)
                                .description("Step " + String.valueOf(stepIndex + 1) + ":" + args.allSelectedSteps.get(stepIndex))
                                .waitFor(firstStageResult.getJacsServiceData()) // for dependency based on previous step
                                .build(),
                        new ServiceArg("-step", args.allSelectedSteps.get(stepIndex).name()),
                        new ServiceArg("-stepIndex", stepIndex + 1),
                        new ServiceArg("-numTimePoints", args.allSelectedTimePoints.get(stepIndex)),
                        new ServiceArg("-timePointsPerJob", timePointsPerJob.toString()),
                        new ServiceArg("-configAddress", args.configAddress)
                );
            })
            ;
        }
        return stage.thenApply((JacsServiceResult<Void> lastStepResult) -> new JacsServiceResult<>(jacsServiceData, lastStepResult.getResult()));
    }

    private LightsheetProcessingArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LightsheetProcessingArgs());
    }
}

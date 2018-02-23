package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Complete lightsheet processing service which invokes multiple LightsheetProcessing steps.
 *
 * @author David Ackerman
 */
@Named("lightsheetProcessing")
public class LightsheetProcessing extends AbstractServiceProcessor<Void> {

    static class LightsheetProcessingArgs extends ServiceArgs {
        @Parameter(names = "-allSelectedStepNames", description = "Selected pipeline steps to run", required = true)
        List<LightsheetPipelineStep> allSelectedSteps;
        @Parameter(names = "-allSelectedTimePoints", description = "Selected time points - the number of selected timepoints must be equal to the number of steps", required = true)
        List<String> allSelectedTimePoints;
    }

    private final WrappedServiceProcessor<LightsheetPipelineStepProcessor, Void> lightsheetPipelineStepProcessor;

    @Inject
    LightsheetProcessing(ServiceComputationFactory computationFactory,
                         JacsServiceDataPersistence jacsServiceDataPersistence,
                         @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                         LightsheetPipelineStepProcessor lightsheetPipelineStepProcessor,
                         Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.lightsheetPipelineStepProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, lightsheetPipelineStepProcessor);

    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LightsheetProcessing.class, new LightsheetProcessingArgs());
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
                new ServiceArg("-timePointsPerJob", timePointsPerJob.toString()));
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
                        new ServiceArg("-timePointsPerJob", timePointsPerJob.toString()));
            })
            ;
        }

        return stage.thenApply((JacsServiceResult<Void> lastStepResult) -> new JacsServiceResult<>(jacsServiceData, lastStepResult.getResult()));
    }

    private LightsheetProcessingArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LightsheetProcessingArgs());
    }
}

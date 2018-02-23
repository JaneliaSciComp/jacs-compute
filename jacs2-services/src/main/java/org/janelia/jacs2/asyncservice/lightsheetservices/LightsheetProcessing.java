package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
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

/**
 * Complete lightsheet processing service which invokes multiple LightsheetProcessing steps.
 *
 * @author David Ackerman
 */
@Named("lightsheetProcessing")
public class LightsheetProcessing extends AbstractServiceProcessor<File> {

    static class LightsheetProcessingArgs extends ServiceArgs {
        @Parameter(names = "-jsonDirectory", description = "Input directory containing JSON files", required = true)
        String jsonDirectory;
        @Parameter(names = "-allSelectedStepNames", description = "Temp", required = true)
        String allSelectedStepNames;
        @Parameter(names = "-allSelectedTimePoints", description = "Temp", required = true)
        String allSelectedTimePoints;
    }

    private final WrappedServiceProcessor<LightsheetPipelineStep, List<File>> lightsheetPipelineStep;

    @Inject
    LightsheetProcessing(ServiceComputationFactory computationFactory,
                         JacsServiceDataPersistence jacsServiceDataPersistence,
                         @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                         LightsheetPipelineStep lightsheetPipelineStep,
                         Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.lightsheetPipelineStep = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, lightsheetPipelineStep);

    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LightsheetProcessing.class, new LightsheetProcessingArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractAnyServiceResultHandler<File>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return true;
            }

            @Override
            public File collectResult(JacsServiceResult<?> depResults) {
                throw new UnsupportedOperationException();
            }

            @Override
            public File getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SampleProcessorResult>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<File>> process(JacsServiceData jacsServiceData) {
        LightsheetProcessingArgs args = getArgs(jacsServiceData);

        final String inputJsonDir = args.jsonDirectory;
        final String[] allSelectedStepNamesArray = args.allSelectedStepNames.split(", ");
        final String[] allSelectedTimePointsArray = args.allSelectedTimePoints.split(", ");
        final Integer timePointsPerJob = 1; //FIXED AT 4

        ServiceComputation<JacsServiceResult<List<File>>> stage = lightsheetPipelineStep.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description(allSelectedStepNamesArray[0])
                        .build(),
                new ServiceArg("-stepName", allSelectedStepNamesArray[0]),
                new ServiceArg("-jsonFile", inputJsonDir + "0_" + allSelectedStepNamesArray[0] + ".json"),
                new ServiceArg("-numTimePoints", allSelectedTimePointsArray[0]),
                new ServiceArg("-timePointsPerJob", timePointsPerJob.toString()));
        for (int i = 1; i < allSelectedStepNamesArray.length; i++) {
            final int final_i = i;
            stage = stage.thenCompose(firstStageResult -> {
                // firstStageResult.getResult
                return lightsheetPipelineStep.process(
                        new ServiceExecutionContext.Builder(jacsServiceData)
                                .waitFor(firstStageResult.getJacsServiceData()) // for dependency based on previous step
                                .description(allSelectedStepNamesArray[final_i])
                                .build(),
                        new ServiceArg("-stepName", allSelectedStepNamesArray[final_i]),
                        new ServiceArg("-jsonFile", inputJsonDir + String.valueOf(final_i) + "_" + allSelectedStepNamesArray[final_i] + ".json"),
                        new ServiceArg("-numTimePoints", allSelectedTimePointsArray[final_i]),
                        new ServiceArg("-timePointsPerJob", timePointsPerJob.toString()));
            })
            ;
        }

        return stage.thenApply((JacsServiceResult<List<File>> fileResult) ->
                new JacsServiceResult<>(jacsServiceData, new File("/home/ackermand/test/junk.txt")))
                .thenApply((JacsServiceResult<File> result) -> {
                    return result;
                });
    }

    private LightsheetProcessingArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LightsheetProcessingArgs());
    }
}

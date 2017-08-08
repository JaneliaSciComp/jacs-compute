package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.janelia.it.jacs.model.domain.DomainConstants;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.sample.ObjectiveSample;
import org.janelia.it.jacs.model.domain.sample.PipelineResult;
import org.janelia.it.jacs.model.domain.sample.SamplePipelineRun;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.fileservices.FileCopyProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.RegisteredJacsNotification;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Named("sampleResultsCompression")
public class SampleResultsCompressionProcessor extends AbstractServiceProcessor<List<PipelineResult>> {

    static class SampleResultsCompressionArgs extends ServiceArgs {
        @Parameter(names = "-sampleId", description = "Sample ID", required = true)
        Long sampleId;
        @Parameter(names = "-objective", description = "Sample objective for which to update the separation result.", required = false)
        String sampleObjective;
        @Parameter(names = "-runId", description = "Run ID to be updated with the corresponding fragment results.", required = false)
        Long pipelineRunId;
        @Parameter(names = "-resultId", description = "Run ID to be updated with the corresponding fragment results.", required = false)
        Long pipelineResultId;
        @Parameter(names = {"-inputFileType"}, description = "Input file type", required = true)
        List<String> inputFileTypes = new ArrayList<>();
        @Parameter(names = {"-outputFileType"}, description = "Output file type", required = true)
        String outputFileType;
        @Parameter(names = "-keepInput", arity = 0, description = "If set the uncompressed input will not be removed", required = false)
        boolean keepInput = false;
    }

    private final SampleDataService sampleDataService;
    private final WrappedServiceProcessor<FileCopyProcessor, File> fileCopyProcessor;

    @Inject
    SampleResultsCompressionProcessor(ServiceComputationFactory computationFactory,
                                      JacsServiceDataPersistence jacsServiceDataPersistence,
                                      @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                      SampleDataService sampleDataService,
                                      FileCopyProcessor fileCopyProcessor,
                                      Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.fileCopyProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, fileCopyProcessor);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SampleResultsCompressionProcessor.class, new SampleResultsCompressionArgs());
    }

    @Override
    public ServiceResultHandler<List<PipelineResult>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<PipelineResult>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @SuppressWarnings("unchecked")
            @Override
            public List<PipelineResult> collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<List<PipelineResult>> intermediateResult = (JacsServiceResult<List<PipelineResult>>)depResults;
                return intermediateResult.getResult();
            }

            @Override
            public List<PipelineResult> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<PipelineResult>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<PipelineResult>>> process(JacsServiceData jacsServiceData) {
        SampleResultsCompressionArgs args = getArgs(jacsServiceData);
        List<ObjectiveSample> objectivesSample =
                sampleDataService.getObjectivesBySampleIdAndObjective(
                        jacsServiceData.getOwner(),
                        args.sampleId,
                        args.sampleObjective);
        List<ServiceComputation<?>> updateResultComputations = new ArrayList<>();
        for (ObjectiveSample objectiveSample : objectivesSample) {
            for(SamplePipelineRun run : objectiveSample.getPipelineRuns()) {
                if (args.pipelineRunId != null &&
                        run.notSameId(args.pipelineRunId)) {
                    continue;
                }
                run.streamResults()
                        .map(indexedResult -> indexedResult.getReference())
                        .filter((PipelineResult result) -> args.pipelineResultId == null || result.sameId(args.pipelineResultId))
                        .forEach((PipelineResult result) -> {
                            setNewResultFile(jacsServiceData, objectiveSample, run, result, args.inputFileTypes, args.outputFileType, !args.keepInput)
                                    .ifPresent(updateResultComputations::add);
                        });
            }
        }
        return computationFactory.newCompletedComputation(null)
                .thenCombineAll(updateResultComputations, (nullResult, prs) -> new JacsServiceResult<>(jacsServiceData, (List<PipelineResult>) prs))
                .thenSuspendUntil(this.suspendCondition(jacsServiceData))
                .thenApply(prsCond -> updateServiceResult(jacsServiceData, prsCond.getState().getResult())) // update the result
                ;
    }

    private SampleResultsCompressionArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SampleResultsCompressionArgs());
    }

    private Optional<ServiceComputation<PipelineResult>> setNewResultFile(JacsServiceData jacsServiceData,
                                                                          ObjectiveSample objectiveSample, SamplePipelineRun run,
                                                                          PipelineResult result, List<String> inputTypes, String outputType,
                                                                          boolean deleteUncompressedInput) {
        Path inputPath = result.getFullFilePath(FileType.LosslessStack);
        String inputExt = inputPath != null ? FileUtils.getFileExtensionOnly(inputPath) : null;
        if (inputExt == null || inputPath.startsWith(DomainConstants.SCALITY_PATH_PREFIX) || inputExt.endsWith(outputType) || inputTypes.stream().noneMatch(inputExt::endsWith)) {
            // the input type is the same as the output or the input is not in the list of input types
            return Optional.empty();
        }
        Path outputPath = getOutput(result, outputType);
        if (outputPath == null || !FileUtils.getFileExtensionOnly(outputPath).endsWith(outputType)) {
            Path newOutputPath = FileUtils.replaceFileExt(inputPath, outputType);
            return Optional.of(
                    fileCopyProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                                    .description("Convert input file")
                                    .registerProcessingNotification(
                                            FlylightSampleEvents.COMPRESS_RESULTS,
                                            new RegisteredJacsNotification().withDefaultLifecycleStages()
                                                    .addNotificationField("sampleId", objectiveSample.getParent().getId())
                                                    .addNotificationField("objective", objectiveSample.getObjective())
                                                    .addNotificationField("run", run.getId())
                                                    .addNotificationField("result", result.getId())
                                                    .addNotificationField("input", inputPath.toString())
                                                    .addNotificationField("output", newOutputPath.toString())
                                                    .addNotificationField("removeInput", String.valueOf(deleteUncompressedInput))
                                    )
                                    .build(),
                            new ServiceArg("-src", inputPath.toString()),
                            new ServiceArg("-dst", newOutputPath.toString())
                    ).thenApply((JacsServiceResult<File> fr) -> {
                        setOutput(result, outputType, newOutputPath);
                        if (deleteUncompressedInput) {
                            result.setFileName(FileType.LosslessStack, null);
                        }
                        sampleDataService.updateSampleObjectivePipelineRunResult(objectiveSample.getParent(), objectiveSample.getObjective(), run.getId(), result);
                        if (deleteUncompressedInput) {
                            try {
                                Files.delete(inputPath);
                            } catch (IOException e) {
                                logger.warn("Failed to delete {}", inputPath, e);
                            }
                        }
                        return result;
                    })
            );
        } else {
            return Optional.empty();
        }
    }

    private Path getOutput(PipelineResult result, String outputType) {
        if ("h5j".equals(outputType)) {
            return result.getFullFilePath(FileType.VisuallyLosslessStack);
        } else {
            return result.getFullFilePath(FileType.LosslessStack);
        }
    }

    private void setOutput(PipelineResult result, String outputType, Path output) {
        if ("h5j".equals(outputType)) {
            result.setFileName(FileType.VisuallyLosslessStack, output.toString());
        } else {
            result.setFileName(FileType.LosslessStack, output.toString());
        }
    }
}

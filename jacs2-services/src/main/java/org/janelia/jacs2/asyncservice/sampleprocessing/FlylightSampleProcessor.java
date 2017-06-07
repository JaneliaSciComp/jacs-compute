package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.IndexedReference;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.sample.NeuronSeparation;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentServiceBuilder;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentServiceBuilderFactory;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentProcessor;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentResultFiles;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentServiceParams;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationFiles;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Named("flylightSample")
public class FlylightSampleProcessor extends AbstractServiceProcessor<Void> {

    private final SampleDataService sampleDataService;
    private final WrappedServiceProcessor<GetSampleImageFilesProcessor, List<SampleImageFile>> getSampleImageFilesProcessor;
    private final WrappedServiceProcessor<SampleLSMSummaryProcessor,List<LSMSummary>> sampleLSMSummaryProcessor;
    private final WrappedServiceProcessor<SampleStitchProcessor, SampleResult> sampleStitchProcessor;
    private final WrappedServiceProcessor<UpdateSamplePipelineResultsProcessor, List<SampleProcessorResult>> updateSamplePipelineResultsProcessor;
    private final WrappedServiceProcessor<SampleNeuronSeparationProcessor, NeuronSeparationFiles> sampleNeuronSeparationProcessor;
    private final WrappedServiceProcessor<AlignmentProcessor, AlignmentResultFiles> alignmentProcessor;
    private final WrappedServiceProcessor<UpdateAlignmentResultsProcessor, AlignmentResultFiles> updateAlignmentResultsProcessor;
    private final AlignmentServiceBuilderFactory alignmentServiceBuilderFactory;

    @Inject
    FlylightSampleProcessor(ServiceComputationFactory computationFactory,
                            JacsServiceDataPersistence jacsServiceDataPersistence,
                            @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                            SampleDataService sampleDataService,
                            GetSampleImageFilesProcessor getSampleImageFilesProcessor,
                            SampleLSMSummaryProcessor sampleLSMSummaryProcessor,
                            SampleStitchProcessor sampleStitchProcessor,
                            UpdateSamplePipelineResultsProcessor updateSamplePipelineResultsProcessor,
                            SampleNeuronSeparationProcessor sampleNeuronSeparationProcessor,
                            AlignmentServiceBuilderFactory alignmentServiceBuilderFactory,
                            AlignmentProcessor alignmentProcessor,
                            UpdateAlignmentResultsProcessor updateAlignmentResultsProcessor,
                            Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.getSampleImageFilesProcessor = new WrappedServiceProcessor(computationFactory, jacsServiceDataPersistence, getSampleImageFilesProcessor);
        this.sampleLSMSummaryProcessor = new WrappedServiceProcessor(computationFactory, jacsServiceDataPersistence, sampleLSMSummaryProcessor);
        this.sampleStitchProcessor = new WrappedServiceProcessor(computationFactory, jacsServiceDataPersistence, sampleStitchProcessor);
        this.updateSamplePipelineResultsProcessor = new WrappedServiceProcessor(computationFactory, jacsServiceDataPersistence, updateSamplePipelineResultsProcessor);
        this.sampleNeuronSeparationProcessor = new WrappedServiceProcessor(computationFactory, jacsServiceDataPersistence, sampleNeuronSeparationProcessor);
        this.alignmentProcessor = new WrappedServiceProcessor(computationFactory, jacsServiceDataPersistence, alignmentProcessor);
        this.updateAlignmentResultsProcessor = new WrappedServiceProcessor(computationFactory, jacsServiceDataPersistence, updateAlignmentResultsProcessor);
        this.alignmentServiceBuilderFactory = alignmentServiceBuilderFactory;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(FlylightSampleProcessor.class, new FlylightSampleArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        FlylightSampleArgs args = getArgs(jacsServiceData);
        String sampleId = args.sampleId.toString();
        Path sampleLsmsSubDir = FileUtils.getDataPath(SampleServicesUtils.DEFAULT_WORKING_LSMS_SUBDIR, jacsServiceData.getId());
        Path sampleSummarySubDir = FileUtils.getDataPath("Summary", jacsServiceData.getId());
        Path sampleStitchingSubDir = FileUtils.getDataPath("Sample", jacsServiceData.getId());

        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenCompose(sd -> {
                            // get sample images
                            ServiceComputation<JacsServiceResult<List<SampleImageFile>>> getSampleImages = getSampleImageFilesProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                                            .description("Retrieve sample LSMs")
                                            .build(),
                                    new ServiceArg("-sampleId", sampleId),
                                    new ServiceArg("-objective", args.sampleObjective),
                                    new ServiceArg("-area", args.sampleArea),
                                    new ServiceArg("-sampleDataRootDir", args.sampleDataRootDir),
                                    new ServiceArg("-sampleLsmsSubDir", sampleLsmsSubDir.toString())
                            );
                            if (!args.skipSummary) {
                                // compute LSM summaries unless the step is skipped
                                getSampleImages.thenCompose((JacsServiceResult<List<SampleImageFile>> lsir) -> sampleLSMSummaryProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                                                        .description("Create sample LSM summary")
                                                        .waitFor(lsir.getJacsServiceData())
                                                        .build(),
                                                new ServiceArg("-sampleId", sampleId),
                                                new ServiceArg("-objective", args.sampleObjective),
                                                new ServiceArg("-area", args.sampleArea),
                                                new ServiceArg("-sampleDataRootDir", args.sampleDataRootDir),
                                                new ServiceArg("-sampleLsmsSubDir", sampleLsmsSubDir.toString()),
                                                new ServiceArg("-sampleSummarySubDir", sampleSummarySubDir.toString()),
                                                new ServiceArg("-channelDyeSpec", args.channelDyeSpec),
                                                new ServiceArg("-basicMipMapsOptions", args.basicMipMapsOptions),
                                                new ServiceArg("-montageMipMaps", args.montageMipMaps)
                                        )
                                );
                            }
                            return getSampleImages;
                        }
                )
                .thenCompose((JacsServiceResult<List<SampleImageFile>> lsir) -> sampleStitchProcessor.process(
                                new ServiceExecutionContext.Builder(jacsServiceData)
                                        .description("Stitch sample tiles")
                                        .waitFor(lsir.getJacsServiceData())
                                        .build(),
                                new ServiceArg("-sampleId", args.sampleId.toString()),
                                new ServiceArg("-objective", args.sampleObjective),
                                new ServiceArg("-area", args.sampleArea),
                                new ServiceArg("-sampleDataRootDir", args.sampleDataRootDir),
                                new ServiceArg("-sampleLsmsSubDir", sampleLsmsSubDir.toString()),
                                new ServiceArg("-sampleSummarySubDir", sampleSummarySubDir.toString()),
                                new ServiceArg("-sampleSitchingSubDir", sampleStitchingSubDir.toString()),
                                new ServiceArg("-mergeAlgorithm", args.mergeAlgorithm),
                                new ServiceArg("-channelDyeSpec", args.channelDyeSpec),
                                new ServiceArg("-outputChannelOrder", args.outputChannelOrder),
                                new ServiceArg("-distortionCorrection", args.applyDistortionCorrection),
                                new ServiceArg("-generateMips", args.persistResults)
                        )
                )
                .thenCompose((JacsServiceResult<SampleResult> stitchResult) -> updateSamplePipelineResultsProcessor.process(
                                new ServiceExecutionContext.Builder(jacsServiceData)
                                        .description("Update sample results")
                                        .waitFor(stitchResult.getJacsServiceData())
                                        .build(),
                                new ServiceArg("-sampleProcessingId", stitchResult.getJacsServiceData().getId().toString())
                        )
                )
                .thenCompose((JacsServiceResult<List<SampleProcessorResult>> lspr) -> {
                    List<ServiceComputation<?>> afterSampleProcessNeuronSeparations;
                    if (args.runNeuronSeparationAfterSampleProcessing) {
                        List<SampleProcessorResult> sampleResults = lspr.getResult();
                        afterSampleProcessNeuronSeparations = IntStream.range(0, sampleResults.size())
                                .mapToObj(pos -> new IndexedReference<>(lspr.getResult().get(pos), pos))
                                .map(indexedSr -> {
                                    SampleProcessorResult sr = indexedSr.getReference();
                                    Path neuronSeparationOutputDir = getNeuronSeparationOutputDir(args.sampleDataRootDir, "Separation", sr.getResultId(), sampleResults.size(), sr.getArea(), indexedSr.getPos());
                                    String previousNeuronsResult = getPreviousNeuronsResultFile(jacsServiceData, sr.getSampleId(), sr.getObjective(), sr.getRunId());
                                    return sampleNeuronSeparationProcessor.process(
                                            new ServiceExecutionContext.Builder(jacsServiceData)
                                                    .description("Separate sample neurons")
                                                    .waitFor(lspr.getJacsServiceData())
                                                    .build(),
                                            new ServiceArg("-sampleId", sr.getSampleId().toString()),
                                            new ServiceArg("-objective", sr.getObjective()),
                                            new ServiceArg("-runId", sr.getRunId().toString()),
                                            new ServiceArg("-resultId", sr.getResultId().toString()),
                                            new ServiceArg("-inputFile", sr.getAreaFile()),
                                            new ServiceArg("-outputDir", neuronSeparationOutputDir.toString()),
                                            new ServiceArg("-signalChannels", sr.getSignalChannels()),
                                            new ServiceArg("-referenceChannel", sr.getReferenceChannel()),
                                            new ServiceArg("-previousResultFile", previousNeuronsResult)
                                    );
                                })
                                .collect(Collectors.toList());
                    } else {
                        afterSampleProcessNeuronSeparations = ImmutableList.<ServiceComputation<?>>of();
                    }
                    return computationFactory.newCompletedComputation(lspr)
                            .thenCombineAll(afterSampleProcessNeuronSeparations, (JacsServiceResult<List<SampleProcessorResult>> lspr1, List<?> results) -> {
                                List<JacsServiceResult<NeuronSeparationFiles>> afterSampleProcessNeuronSeparationsResults = (List<JacsServiceResult<NeuronSeparationFiles>>) results;
                                AlignmentServiceBuilder alignmentServiceBuilder = null;
                                if (StringUtils.isNotBlank(args.alignmentAlgorithm)) {
                                    alignmentServiceBuilder = alignmentServiceBuilderFactory.getServiceArgBuilder(args.alignmentAlgorithm);
                                }
                                if (alignmentServiceBuilder != null) {
                                    List<AlignmentServiceParams> alignmentServicesParams =
                                            alignmentServiceBuilder.getAlignmentServicesArgs(
                                                    args.alignmentAlgorithm,
                                                    args.sampleDataRootDir,
                                                    lspr1.getResult(),
                                                    afterSampleProcessNeuronSeparationsResults.stream().map(JacsServiceResult::getResult).collect(Collectors.toList()));
                                    int resultIndex = 0;
                                    for (AlignmentServiceParams alignmentServiceParams : alignmentServicesParams) {
                                        JacsServiceResult<NeuronSeparationFiles> neuronSeparationResult = afterSampleProcessNeuronSeparationsResults.get(resultIndex);
                                        alignmentProcessor.process(
                                                new ServiceExecutionContext.Builder(jacsServiceData)
                                                        .description("Align sample")
                                                        .waitFor(lspr1.getJacsServiceData(), neuronSeparationResult.getJacsServiceData())
                                                        .build(),
                                                alignmentServiceParams.getAlignmentServiceArgs().toArray(new ServiceArg[alignmentServiceParams.getAlignmentServiceArgs().size()])
                                        )
                                        .thenCompose((JacsServiceResult<AlignmentResultFiles> alignmentResult) -> {
                                            return updateAlignmentResultsProcessor.process(
                                                    new ServiceExecutionContext.Builder(jacsServiceData)
                                                            .description("Update alignment results")
                                                            .waitFor(neuronSeparationResult.getJacsServiceData())
                                                            .build(),
                                                    new ServiceArg("-sampleId", alignmentServiceParams.getSampleProcessorResult().getSampleId().toString()),
                                                    new ServiceArg("-objective", alignmentServiceParams.getSampleProcessorResult().getObjective()),
                                                    new ServiceArg("-area", alignmentServiceParams.getSampleProcessorResult().getArea()),
                                                    new ServiceArg("-runId", alignmentServiceParams.getSampleProcessorResult().getRunId().toString()),
                                                    new ServiceArg("-alignmentServiceId", alignmentResult.getJacsServiceData().getId().toString())
                                            );
                                        });
                                        ++resultIndex;
                                    }
                                    return lspr1;

                                } else {
                                    return lspr1;
                                }
                            });
                })
                .thenSuspendUntil(r -> new ContinuationCond.Cond<>(r, !suspendUntilAllDependenciesComplete(jacsServiceData)))
                .thenApply(r -> new JacsServiceResult<Void>(jacsServiceData));
            }


    private FlylightSampleArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(jacsServiceData.getArgsArray(), new FlylightSampleArgs());
    }

    private Path getNeuronSeparationOutputDir(String sampleDataRootDir, String topSubDir, Number parentResultId, int nAreas, String area, int resultIndex) {
        Path neuronSeparationOutputDir;
        if (StringUtils.isNotBlank(area)) {
            neuronSeparationOutputDir = Paths.get(sampleDataRootDir)
                    .resolve(FileUtils.getDataPath(topSubDir, parentResultId))
                    .resolve(area);
        } else if (nAreas > 1) {
            neuronSeparationOutputDir = Paths.get(sampleDataRootDir)
                    .resolve(FileUtils.getDataPath(topSubDir, parentResultId))
                    .resolve("area" + resultIndex + 1);
        } else {
            neuronSeparationOutputDir = Paths.get(sampleDataRootDir)
                    .resolve(FileUtils.getDataPath(topSubDir, parentResultId));
        }
        return neuronSeparationOutputDir;
    }

    private String getPreviousNeuronsResultFile(JacsServiceData jacsServiceData, Number sampleId, String objective, Number runId) {
        // check previus neuron separation results and return corresponding result file
        Sample sample = sampleDataService.getSampleById(jacsServiceData.getOwner(), sampleId);
        return sample.lookupObjective(objective)
                .flatMap(objectiveSample -> objectiveSample.findPipelineRunById(runId))
                .map(indexedRun -> indexedRun.getReference())
                .map(sampleRun -> sampleRun.streamResults()
                        .map(indexedResult -> indexedResult.getReference())
                        .filter(result -> result instanceof NeuronSeparation) // only look at neuronseparation result types
                        .sorted((pr1, pr2) -> pr2.getCreationDate().compareTo(pr1.getCreationDate())) // sort by creation date desc
                        .map(ns -> ns.getFileName(FileType.NeuronSeparatorResult))
                        .filter(nsfn -> StringUtils.isNoneBlank(nsfn)) // filter out result that don't have a neuron separation result
                        .findFirst()
                        .orElse(null))
                .orElse((String) null);
    }
}

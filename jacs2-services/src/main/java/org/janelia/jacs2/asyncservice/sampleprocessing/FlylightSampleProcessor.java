package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.IndexedReference;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.sample.LSMSummaryResult;
import org.janelia.it.jacs.model.domain.sample.NeuronSeparation;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.it.jacs.model.domain.sample.SampleAlignmentResult;
import org.janelia.it.jacs.model.domain.sample.SamplePipelineRun;
import org.janelia.it.jacs.model.domain.sample.SamplePostProcessingResult;
import org.janelia.it.jacs.model.domain.sample.SampleProcessingResult;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentProcessor;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentResultFiles;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentServiceBuilder;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentServiceBuilderFactory;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentServiceParams;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
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
import org.janelia.jacs2.asyncservice.imageservices.AbstractMIPsAndMoviesProcessor;
import org.janelia.jacs2.asyncservice.imageservices.BasicMIPsAndMoviesProcessor;
import org.janelia.jacs2.asyncservice.imageservices.EnhancedMIPsAndMoviesProcessor;
import org.janelia.jacs2.asyncservice.imageservices.FijiUtils;
import org.janelia.jacs2.asyncservice.imageservices.MIPsAndMoviesInput;
import org.janelia.jacs2.asyncservice.imageservices.MIPsAndMoviesResult;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationFiles;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.RegisteredJacsNotification;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Named("flylightSample")
public class FlylightSampleProcessor extends AbstractServiceProcessor<List<SampleProcessorResult>> {

    private final SampleDataService sampleDataService;
    private final WrappedServiceProcessor<InitializeSamplePipelineResultsProcessor, List<SamplePipelineRun>> initializeSamplePipelineResultsProcessor;
    private final WrappedServiceProcessor<GetSampleImageFilesProcessor, List<SampleImageFile>> getSampleImageFilesProcessor;
    private final WrappedServiceProcessor<SampleLSMSummaryProcessor,List<LSMSummary>> sampleLSMSummaryProcessor;
    private final WrappedServiceProcessor<UpdateSampleSummaryResultsProcessor, List<LSMSummaryResult>> updateSampleSummaryResultsProcessor;
    private final WrappedServiceProcessor<SampleStitchProcessor, SampleResult> sampleStitchProcessor;
    private final WrappedServiceProcessor<UpdateSampleProcessingResultsProcessor, List<SampleProcessorResult>> updateSampleProcessingResultsProcessor;
    private final WrappedServiceProcessor<BasicMIPsAndMoviesProcessor, MIPsAndMoviesResult> basicMIPsAndMoviesProcessor;
    private final WrappedServiceProcessor<EnhancedMIPsAndMoviesProcessor, MIPsAndMoviesResult> enhancedMIPsAndMoviesProcessor;
    private final WrappedServiceProcessor<UpdateSamplePostProcessingResultsProcessor, SamplePostProcessingResult> updateSamplePostProcessingResultsProcessor;
    private final WrappedServiceProcessor<SampleNeuronSeparationProcessor, NeuronSeparationFiles> sampleNeuronSeparationProcessor;
    private final WrappedServiceProcessor<AlignmentProcessor, AlignmentResultFiles> alignmentProcessor;
    private final WrappedServiceProcessor<UpdateAlignmentResultsProcessor, AlignmentResult> updateAlignmentResultsProcessor;
    private final WrappedServiceProcessor<SampleNeuronWarpingProcessor, NeuronSeparationFiles> sampleNeuronWarpingProcessor;
    private final WrappedServiceProcessor<CleanSampleImageFilesProcessor, Void> cleanSampleImageFilesProcessor;
    private final AlignmentServiceBuilderFactory alignmentServiceBuilderFactory;

    @Inject
    FlylightSampleProcessor(ServiceComputationFactory computationFactory,
                            JacsServiceDataPersistence jacsServiceDataPersistence,
                            @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                            SampleDataService sampleDataService,
                            InitializeSamplePipelineResultsProcessor initializeSamplePipelineResultsProcessor,
                            GetSampleImageFilesProcessor getSampleImageFilesProcessor,
                            SampleLSMSummaryProcessor sampleLSMSummaryProcessor,
                            UpdateSampleSummaryResultsProcessor updateSampleSummaryResultsProcessor,
                            SampleStitchProcessor sampleStitchProcessor,
                            UpdateSampleProcessingResultsProcessor updateSampleProcessingResultsProcessor,
                            BasicMIPsAndMoviesProcessor basicMIPsAndMoviesProcessor,
                            EnhancedMIPsAndMoviesProcessor enhancedMIPsAndMoviesProcessor,
                            UpdateSamplePostProcessingResultsProcessor updateSamplePostProcessingResultsProcessor,
                            SampleNeuronSeparationProcessor sampleNeuronSeparationProcessor,
                            AlignmentServiceBuilderFactory alignmentServiceBuilderFactory,
                            AlignmentProcessor alignmentProcessor,
                            UpdateAlignmentResultsProcessor updateAlignmentResultsProcessor,
                            SampleNeuronWarpingProcessor sampleNeuronWarpingProcessor,
                            CleanSampleImageFilesProcessor cleanSampleImageFilesProcessor,
                            Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.initializeSamplePipelineResultsProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, initializeSamplePipelineResultsProcessor);
        this.getSampleImageFilesProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, getSampleImageFilesProcessor);
        this.sampleLSMSummaryProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, sampleLSMSummaryProcessor);
        this.updateSampleSummaryResultsProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, updateSampleSummaryResultsProcessor);
        this.sampleStitchProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, sampleStitchProcessor);
        this.updateSampleProcessingResultsProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, updateSampleProcessingResultsProcessor);
        this.basicMIPsAndMoviesProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, basicMIPsAndMoviesProcessor);
        this.enhancedMIPsAndMoviesProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, enhancedMIPsAndMoviesProcessor);
        this.updateSamplePostProcessingResultsProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, updateSamplePostProcessingResultsProcessor);
        this.sampleNeuronSeparationProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, sampleNeuronSeparationProcessor);
        this.alignmentProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, alignmentProcessor);
        this.updateAlignmentResultsProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, updateAlignmentResultsProcessor);
        this.sampleNeuronWarpingProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, sampleNeuronWarpingProcessor);
        this.cleanSampleImageFilesProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, cleanSampleImageFilesProcessor);
        this.alignmentServiceBuilderFactory = alignmentServiceBuilderFactory;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(FlylightSampleProcessor.class, new FlylightSampleArgs());
    }

    @Override
    public ServiceResultHandler<List<SampleProcessorResult>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<SampleProcessorResult>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public List<SampleProcessorResult> collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<List<SampleProcessorResult>> intermediateResult = (JacsServiceResult<List<SampleProcessorResult>>)depResults;
                return intermediateResult.getResult();
            }

            @Override
            public List<SampleProcessorResult> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SampleProcessorResult>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<SampleProcessorResult>>> process(JacsServiceData jacsServiceData) {
        FlylightSampleArgs args = getArgs(jacsServiceData);
        Path sampleLsmsSubDir = FileUtils.getDataPath(SampleServicesUtils.DEFAULT_WORKING_LSMS_SUBDIR, jacsServiceData.getId());
        Path sampleSummarySubDir = FileUtils.getDataPath("Summary", jacsServiceData.getId());
        Path sampleStitchingSubDir = FileUtils.getDataPath("Sample", jacsServiceData.getId());
        Path samplePostProcessingSubDir = FileUtils.getDataPath("Post", jacsServiceData.getId());

        Number sampleResultsId = getSampleResultsId(jacsServiceData, args);

        ServiceComputation<JacsServiceResult<List<SampleImageFile>>> initAndGetSampleImagesComputation =
                initSampleResults(jacsServiceData, args.sampleId, args.sampleObjective, sampleResultsId, args.sampleResultsName, args.sampleProcessName)
                        .thenCompose(ignored -> getSampleImages(jacsServiceData, args.sampleId, args.sampleObjective, args.sampleArea, args.sampleDataRootDir, sampleLsmsSubDir));

        if (!args.skipSummary) {
            // compute LSM summaries in parallel with everything else
            initAndGetSampleImagesComputation
                    .thenCompose((JacsServiceResult<List<SampleImageFile>> lsir) -> sampleLSMSummaryProcessor.process(
                            new ServiceExecutionContext.Builder(jacsServiceData)
                                    .description("Create sample LSM summary")
                                    .waitFor(lsir.getJacsServiceData())
                                    .registerProcessingStageNotification(
                                            FlylightSampleEvents.LSM_METADATA,
                                            jacsServiceData.getProcessingStageNotification(FlylightSampleEvents.LSM_METADATA, new RegisteredJacsNotification().withDefaultLifecycleStages())
                                                    .map(n -> n.addNotificationField("sampleId", args.sampleId)
                                                                    .addNotificationField("objective", args.sampleObjective)
                                                                    .addNotificationField("area", args.sampleArea)
                                                    )
                                    )
                                    .registerProcessingStageNotification(
                                            FlylightSampleEvents.SUMMARY_MIPMAPS,
                                            jacsServiceData.getProcessingStageNotification(FlylightSampleEvents.SUMMARY_MIPMAPS, new RegisteredJacsNotification().withDefaultLifecycleStages())
                                                    .map(n -> n.addNotificationField("sampleId", args.sampleId)
                                                                    .addNotificationField("objective", args.sampleObjective)
                                                                    .addNotificationField("area", args.sampleArea)
                                                    )
                                    )
                                   .build(),
                                            new ServiceArg("-sampleId", args.sampleId),
                                            new ServiceArg("-objective", args.sampleObjective),
                                            new ServiceArg("-area", args.sampleArea),
                                            new ServiceArg("-sampleResultsId", sampleResultsId),
                                            new ServiceArg("-sampleDataRootDir", args.sampleDataRootDir),
                                            new ServiceArg("-sampleLsmsSubDir", sampleLsmsSubDir.toString()),
                                            new ServiceArg("-sampleSummarySubDir", sampleSummarySubDir.toString()),
                                            new ServiceArg("-channelDyeSpec", args.channelDyeSpec),
                                            new ServiceArg("-basicMipMapsOptions", args.basicMipMapsOptions),
                                            new ServiceArg("-montageMipMaps", args.montageMipMaps)
                                    )
                    )
                            .thenCompose((JacsServiceResult<List<LSMSummary>> lsmSummariesResult) -> updateSampleSummaryResultsProcessor.process(
                                            new ServiceExecutionContext.Builder(jacsServiceData)
                                                    .description("Update LSM summary results")
                                                    .waitFor(lsmSummariesResult.getJacsServiceData())
                                                    .build(),
                                            new ServiceArg("-sampleResultsId", sampleResultsId),
                                            new ServiceArg("-sampleSummaryId", lsmSummariesResult.getJacsServiceData().getId())
                                    )
                            )
            ;
        }
        return initAndGetSampleImagesComputation
                .thenCompose((JacsServiceResult<List<SampleImageFile>> lsir) -> processSampleImages( // process sample
                        jacsServiceData, args.sampleId, args.sampleObjective, args.sampleArea, sampleResultsId,
                        args.sampleDataRootDir, sampleLsmsSubDir, sampleSummarySubDir, sampleStitchingSubDir, samplePostProcessingSubDir,
                        args.mergeAlgorithm, args.channelDyeSpec, args.outputChannelOrder,
                        args.applyDistortionCorrection, args.persistResults,
                        args.imageType, args.postProcessingMipMapsOptions, args.defaultPostProcessingColorSpec,
                        lsir.getJacsServiceData()))
                .thenCompose((JacsServiceResult<List<SampleProcessorResult>> lspr) -> {
                    List<ServiceComputation<JacsServiceResult<NeuronSeparationFiles>>> neuronSeparationComputations = runSampleNeuronSeparations(
                            jacsServiceData,
                            lspr.getResult(),
                            (args.runNeuronSeparationAfterSampleProcessing ? lspr.getResult().size() : 0),
                            args.sampleDataRootDir,
                            lspr.getJacsServiceData());
                    if (CollectionUtils.isEmpty(args.alignmentAlgorithms)) {
                        // no alignment requested so stop here
                        return computationFactory.newCompletedComputation(lspr);
                    }
                    // run all requested alignments followed by the neuron separations
                    List<ServiceComputation<?>> afterSampleProcessNeuronSeparationsResults = ImmutableList.copyOf(neuronSeparationComputations);
                    return computationFactory.newCompletedComputation(lspr)
                            .thenCombineAll(afterSampleProcessNeuronSeparationsResults, (JacsServiceResult<List<SampleProcessorResult>> lspr1, List<?> results) -> {
                                // after all neuron separation complete run the alignments
                                @SuppressWarnings("unchecked")
                                List<JacsServiceResult<NeuronSeparationFiles>> neuronSeparationsResults = (List<JacsServiceResult<NeuronSeparationFiles>>) results;
                                runAllAlignments(args.sampleId, jacsServiceData, args.sampleDataRootDir, args.alignmentAlgorithms, lspr1, neuronSeparationsResults);
                                return lspr1;
                            });
                })
                .thenSuspendUntil((JacsServiceResult<List<SampleProcessorResult>> lspr) -> new ContinuationCond.Cond<>(lspr, !suspendUntilAllDependenciesComplete(jacsServiceData))) // wait for all subtasks to complete
                .thenApply((ContinuationCond.Cond<JacsServiceResult<List<SampleProcessorResult>>> lsprCond) -> {
                    cleanSampleImageFilesProcessor.process(
                            new ServiceExecutionContext.Builder(jacsServiceData)
                                    .description("Remove working LSMs")
                                    .waitFor(lsprCond.getState().getJacsServiceData())
                                    .build(),
                            new ServiceArg("-sampleId", args.sampleId),
                            new ServiceArg("-objective", args.sampleObjective),
                            new ServiceArg("-area", args.sampleArea),
                            new ServiceArg("-sampleDataRootDir", args.sampleDataRootDir),
                            new ServiceArg("-sampleLsmsSubDir", sampleLsmsSubDir.toString()),
                            new ServiceArg("-sampleSummarySubDir", sampleSummarySubDir.toString()),
                            new ServiceArg("-sampleSitchingSubDir", sampleStitchingSubDir.toString())
                    );
                    return lsprCond.getState();
                })
                .thenApply((JacsServiceResult<List<SampleProcessorResult>> lspr) -> this.updateServiceResult(jacsServiceData, lspr.getResult()))  // update the Flylight result
                ;
    }

    private ServiceComputation<JacsServiceResult<List<SamplePipelineRun>>> initSampleResults(JacsServiceData jacsServiceData, Number sampleId, String sampleObjective,
                                                                                             Number resultsId, String sampleResultName, String sampleProcessName) {
        // results name defaults to service description and then to service name if there's no description
        // process name default to service name
        // results ID default to service ID
        return initializeSamplePipelineResultsProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Initialize pipeline run")
                        .build(),
                new ServiceArg("-sampleId", sampleId),
                new ServiceArg("-objective", sampleObjective),
                new ServiceArg("-sampleResultsName", StringUtils.defaultIfBlank(sampleResultName, StringUtils.defaultIfBlank(jacsServiceData.getDescription(), jacsServiceData.getName()))),
                new ServiceArg("-sampleProcessName", StringUtils.defaultIfBlank(sampleProcessName, jacsServiceData.getName())),
                new ServiceArg("-sampleResultsId", resultsId)
        );
    }

    private Number getSampleResultsId(JacsServiceData jacsServiceData, FlylightSampleArgs args) {
        return args.sampleResultsId == null ? jacsServiceData.getId() : args.sampleResultsId;
    }

    private ServiceComputation<JacsServiceResult<List<SampleImageFile>>> getSampleImages(JacsServiceData jacsServiceData, Number sampleId, String sampleObjective, String sampleArea,
                                                                                         String sampleDataRootDir, Path sampleLsmsSubDir) {
        // get sample images
        return getSampleImageFilesProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Retrieve sample LSMs")
                        .build(),
                new ServiceArg("-sampleId", sampleId),
                new ServiceArg("-objective", sampleObjective),
                new ServiceArg("-area", sampleArea),
                new ServiceArg("-sampleDataRootDir", sampleDataRootDir),
                new ServiceArg("-sampleLsmsSubDir", sampleLsmsSubDir.toString())
        );
    }

    private ServiceComputation<JacsServiceResult<List<SampleProcessorResult>>> processSampleImages(JacsServiceData jacsServiceData, Number sampleId, String sampleObjective, String sampleArea,
                                                                                                   Number sampleResultsId, String sampleDataRootDir, Path sampleLsmsSubDir, Path sampleSummarySubDir,
                                                                                                   Path sampleStitchingSubDir, Path samplePostProcessingSubDir,
                                                                                                   String mergeAlgorithm, String channelDyeSpec, String outputChannelOrder,
                                                                                                   boolean applyDistortionCorrection, boolean generateMips,
                                                                                                   String imageType, String postProcessingMipMapsOptions, String defaultPostProcessingColorSpec,
                                                                                                   JacsServiceData... deps) {
        ServiceComputation<JacsServiceResult<SampleResult>> processingSample = sampleStitchProcessor.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Stitch sample tiles")
                        .waitFor(deps)
                        .registerProcessingStageNotification(
                                FlylightSampleEvents.MERGE_LSMS,
                                jacsServiceData.getProcessingStageNotification(FlylightSampleEvents.MERGE_LSMS, new RegisteredJacsNotification().withDefaultLifecycleStages())
                                        .map(n -> n.addNotificationField("sampleId", sampleId)
                                                        .addNotificationField("objective", sampleObjective)
                                                        .addNotificationField("area", sampleArea)
                                        )
                        )
                        .registerProcessingStageNotification(
                                FlylightSampleEvents.STITCH_TILES,
                                jacsServiceData.getProcessingStageNotification(FlylightSampleEvents.STITCH_TILES, new RegisteredJacsNotification().withDefaultLifecycleStages())
                                        .map(n -> n.addNotificationField("sampleId", sampleId)
                                                        .addNotificationField("objective", sampleObjective)
                                                        .addNotificationField("area", sampleArea)
                                        )
                        )
                        .build(),
                new ServiceArg("-sampleId", sampleId),
                new ServiceArg("-objective", sampleObjective),
                new ServiceArg("-area", sampleArea),
                new ServiceArg("-sampleResultsId", sampleResultsId),
                new ServiceArg("-sampleDataRootDir", sampleDataRootDir),
                new ServiceArg("-sampleLsmsSubDir", sampleLsmsSubDir.toString()),
                new ServiceArg("-sampleSummarySubDir", sampleSummarySubDir.toString()),
                new ServiceArg("-sampleSitchingSubDir", sampleStitchingSubDir.toString()),
                new ServiceArg("-mergeAlgorithm", mergeAlgorithm),
                new ServiceArg("-channelDyeSpec", channelDyeSpec),
                new ServiceArg("-outputChannelOrder", outputChannelOrder),
                new ServiceArg("-distortionCorrection", applyDistortionCorrection),
                new ServiceArg("-generateMips", generateMips)
        );

        ServiceComputation<JacsServiceResult<List<SampleProcessorResult>>> updateProcessingSampleResults =
                processingSample.thenCompose((JacsServiceResult<SampleResult> stitchResult) -> updateSampleProcessingResultsProcessor.process(
                                new ServiceExecutionContext.Builder(jacsServiceData)
                                        .description("Update sample results")
                                        .waitFor(stitchResult.getJacsServiceData())
                                        .build(),
                                new ServiceArg("-sampleResultsId", sampleResultsId),
                                new ServiceArg("-sampleProcessingId", stitchResult.getJacsServiceData().getId())
                        )
                );

        processingSample
                .thenCombine(updateProcessingSampleResults, (JacsServiceResult<SampleResult> sampleResult, JacsServiceResult<List<SampleProcessorResult>> updateSampleResults) -> {
                    Multimap<String, SampleAreaResult> objectiveAreas = Multimaps.index(sampleResult.getResult().getSampleAreaResults(), SampleAreaResult::getObjective);
                    return objectiveAreas.asMap().entrySet().stream()
                            .map(objectiveAreasEntry -> {
                                List<MIPsAndMoviesInput> mipsAndMoviesInputs =
                                        objectiveAreasEntry.getValue().stream()
                                                .filter(sar -> !sar.getAnatomicalArea().contains("-Verify"))
                                                .flatMap(sar -> sar.getMipsAndMoviesInput().stream())
                                                .sorted((sar1, sar2) -> ComparisonChain.start()
                                                        .compare(sar1.getArea(), sar2.getArea(), Ordering.natural()) // Brain before VNC
                                                        .compare(sar1.getFilepath(), sar2.getFilepath(), Ordering.natural().nullsLast())  // Order by filename
                                                        .result())
                                                .collect(Collectors.toList());
                                if (objectiveAreasEntry.getKey().equals("20x") && mipsAndMoviesInputs.size() == 2 &&
                                        mipsAndMoviesInputs.stream().map(MIPsAndMoviesInput::getArea).collect(Collectors.toSet()).equals(ImmutableSet.of("Brain", "VNC"))) {
                                    // Special case - if there are exactly 2 20x tile - one Brain and one VNC then generate normalized mipmaps
                                    MIPsAndMoviesInput brainArea = mipsAndMoviesInputs.stream().filter(sar -> "Brain".equals(sar.getArea())).findFirst().orElseThrow(IllegalStateException::new);
                                    MIPsAndMoviesInput vncArea = mipsAndMoviesInputs.stream().filter(sar -> "VNC".equals(sar.getArea())).findFirst().orElseThrow(IllegalStateException::new);
                                    Path postProcessingResultsDir = getPostProcessingOutputDir(
                                            sampleDataRootDir,
                                            samplePostProcessingSubDir,
                                            objectiveAreasEntry.getKey(),
                                            "NormalizedBrainVNC",
                                            0);
                                    return basicMIPsAndMoviesProcessor.process(
                                            new ServiceExecutionContext.Builder(jacsServiceData)
                                                    .description("Generate post process mipmaps")
                                                    .waitFor(sampleResult.getJacsServiceData(), updateSampleResults.getJacsServiceData())
                                                    .build(),
                                            new ServiceArg("-imgFile", brainArea.getFilepath()),
                                            new ServiceArg("-imgFilePrefix", brainArea.getOutputPrefix()),
                                            new ServiceArg("-secondImgFile", vncArea.getFilepath()),
                                            new ServiceArg("-secondImgFilePrefix", vncArea.getOutputPrefix()),
                                            new ServiceArg("-mode", imageType),
                                            new ServiceArg("-chanSpec", brainArea.getChanspec()),
                                            new ServiceArg("-colorSpec", StringUtils.defaultIfBlank(defaultPostProcessingColorSpec, getPostProcessingColorSpec(imageType, sampleObjective, brainArea.getChanspec()))),
                                            new ServiceArg("-resultsDir", postProcessingResultsDir.toString()),
                                            new ServiceArg("-options", postProcessingMipMapsOptions)
                                    ).thenCompose((JacsServiceResult<MIPsAndMoviesResult> mipsAndMoviesResult) -> updateSamplePostProcessingResultsProcessor.process(
                                            new ServiceExecutionContext.Builder(jacsServiceData)
                                                    .description("Update post process results")
                                                    .waitFor(mipsAndMoviesResult.getJacsServiceData())
                                                    .build(),
                                            new ServiceArg("-sampleId", sampleId),
                                            new ServiceArg("-objective", objectiveAreasEntry.getKey()),
                                            new ServiceArg("-sampleResultsId", sampleResultsId),
                                            new ServiceArg("-sampleDataRootDir", sampleDataRootDir),
                                            new ServiceArg("-samplePostSubDir", samplePostProcessingSubDir.toString()),
                                            new ServiceArg("-resultDirs", postProcessingResultsDir.toString())
                                    ));
                                } else {
                                    // generate mipmaps for each image
                                    List<ServiceComputation<?>> mipsAndMoviesComputations =
                                            IndexedReference.indexListContent(mipsAndMoviesInputs, (pos, mipsInput) -> new IndexedReference<>(mipsInput, pos))
                                                    .map((IndexedReference<MIPsAndMoviesInput, Integer> indexedMipsInput) -> {
                                                        Path postProcessingResultsDir = getPostProcessingOutputDir(
                                                                sampleDataRootDir,
                                                                samplePostProcessingSubDir,
                                                                objectiveAreasEntry.getKey(),
                                                                indexedMipsInput.getReference().getArea(),
                                                                indexedMipsInput.getPos());
                                                        return selectMIPsAndMoviesProcessor(objectiveAreasEntry.getKey()).process(
                                                                new ServiceExecutionContext.Builder(jacsServiceData)
                                                                        .description("Generate post process mipmaps")
                                                                        .waitFor(sampleResult.getJacsServiceData(), updateSampleResults.getJacsServiceData())
                                                                        .build(),
                                                                new ServiceArg("-imgFile", indexedMipsInput.getReference().getFilepath()),
                                                                new ServiceArg("-mode", imageType),
                                                                new ServiceArg("-chanSpec", indexedMipsInput.getReference().getChanspec()),
                                                                new ServiceArg("-colorSpec", StringUtils.defaultIfBlank(defaultPostProcessingColorSpec, getPostProcessingColorSpec(imageType, sampleObjective, indexedMipsInput.getReference().getChanspec()))),
                                                                new ServiceArg("-resultsDir", postProcessingResultsDir.toString()),
                                                                new ServiceArg("-options", postProcessingMipMapsOptions)
                                                        );
                                                    })
                                                    .collect(Collectors.toList());
                                    return computationFactory.newCompletedComputation(null)
                                            .thenComposeAll(mipsAndMoviesComputations, (empty, results) -> {
                                                @SuppressWarnings("unchecked")
                                                List<JacsServiceResult<MIPsAndMoviesResult>> mipsAndMoviesResults = (List<JacsServiceResult<MIPsAndMoviesResult>>) results;
                                                return updateSamplePostProcessingResultsProcessor.process(
                                                        new ServiceExecutionContext.Builder(jacsServiceData)
                                                                .description("Update post process results")
                                                                .waitFor(mipsAndMoviesResults.stream().map(JacsServiceResult::getJacsServiceData).collect(Collectors.toList()))
                                                                .build(),
                                                        new ServiceArg("-sampleId", sampleId),
                                                        new ServiceArg("-objective", objectiveAreasEntry.getKey()),
                                                        new ServiceArg("-sampleResultsId", sampleResultsId),
                                                        new ServiceArg("-sampleDataRootDir", sampleDataRootDir),
                                                        new ServiceArg("-samplePostSubDir", samplePostProcessingSubDir.toString()),
                                                        new ServiceArg("-resultDirs", mipsAndMoviesResults.stream().map(r -> r.getResult().getResultsDir()).collect(Collectors.joining(",")))
                                                );
                                            });
                                }
                            })
                            .collect(Collectors.toList());
                });
        return updateProcessingSampleResults;
    }

    private WrappedServiceProcessor<? extends AbstractMIPsAndMoviesProcessor, MIPsAndMoviesResult> selectMIPsAndMoviesProcessor(String objective) {
        if ("20x".equals(objective)) {
            return basicMIPsAndMoviesProcessor;
        } else {
            return enhancedMIPsAndMoviesProcessor;
        }
    }

    private String getPostProcessingColorSpec(String mode, String objective, String chanSpec) {
        logger.warn("No OUTPUT_COLOR_SPEC specified, attempting to guess based on objective={} and MODE={} ...", objective, mode);

        if ("mcfo".equals(mode)) {
            // MCFO is always RGB
            return FijiUtils.getDefaultColorSpecAsString(chanSpec, "RGB", '1');
        } else {
            if (!"polarity".equals(mode) & chanSpec.length() == 4) {
                // 4 channel image with unknown mode, let's assume its MCFO
                return FijiUtils.getDefaultColorSpecAsString(chanSpec, "RGB", '1');
            } else {
                // Polarity and other image types (e.g. screen?) get the Yoshi treatment,
                // with green signal on top of magenta reference for 20x and green signal on top of grey reference for 63x.
                if ("20x".equals(objective)) {
                    return FijiUtils.getDefaultColorSpecAsString(chanSpec, "GYC", 'M');
                } else if ("63x".equals(objective)) {
                    if (chanSpec.length() == 2) {
                        return FijiUtils.getDefaultColorSpecAsString(chanSpec, "G", '1');
                    } else {
                        return FijiUtils.getDefaultColorSpecAsString(chanSpec, "MGR", '1');
                    }
                } else {
                    return FijiUtils.getDefaultColorSpecAsString(chanSpec, "RGB", '1'); // default to RGB
                }
            }
        }
    }

    private Path getPostProcessingOutputDir(String sampleDataRootDir, Path postProcessingSubDir, String objective, String area, int resultIndex) {
        Path postProcessingOutputDir = Paths.get(sampleDataRootDir).resolve(postProcessingSubDir);
        if (StringUtils.isNotBlank(objective)) {
            postProcessingOutputDir = postProcessingOutputDir.resolve(objective);
        }
        if (StringUtils.isBlank(area)) {
            postProcessingOutputDir = postProcessingOutputDir.resolve(String.valueOf(resultIndex + 1));
        } else {
            postProcessingOutputDir = postProcessingOutputDir.resolve(area);
        }
        return postProcessingOutputDir;
    }

    private List<ServiceComputation<JacsServiceResult<NeuronSeparationFiles>>> runSampleNeuronSeparations(JacsServiceData jacsServiceData,
                                                                                                          List<SampleProcessorResult> sampleResults, int nSampleResults,
                                                                                                          String sampleDataRootDir,
                                                                                                          JacsServiceData... deps) {
        return IntStream.range(0, nSampleResults)
                    .mapToObj(pos -> new IndexedReference<>(sampleResults.get(pos), pos))
                    .map(indexedSr -> {
                        SampleProcessorResult sr = indexedSr.getReference();
                        Path neuronSeparationOutputDir = getNeuronSeparationOutputDir(sampleDataRootDir, "Separation", sr.getResultId(), sr.getArea(), indexedSr.getPos());
                        Path previousNeuronsResult = getPreviousSampleProcessingBasedNeuronsResultFile(jacsServiceData, sr.getSampleId(), sr.getObjective(), sr.getArea(), sr.getRunId());
                        return sampleNeuronSeparationProcessor.process(
                                new ServiceExecutionContext.Builder(jacsServiceData)
                                        .description("Separate sample neurons")
                                        .addRequiredMemoryInGB(getRequiredMemoryForNeuronSeparationByObjective(sr.getObjective()))
                                        .waitFor(deps)
                                        .build(),
                                new ServiceArg("-sampleId", sr.getSampleId()),
                                new ServiceArg("-objective", sr.getObjective()),
                                new ServiceArg("-runId", sr.getRunId()),
                                new ServiceArg("-resultId", sr.getResultId()),
                                new ServiceArg("-inputFile", sr.getAreaFile()),
                                new ServiceArg("-outputDir", neuronSeparationOutputDir.toString()),
                                new ServiceArg("-signalChannels", sr.getSignalChannels()),
                                new ServiceArg("-referenceChannel", sr.getReferenceChannel()),
                                new ServiceArg("-previousResultFile", previousNeuronsResult != null ? previousNeuronsResult.toString() : "")
                        );
                    })
                    .collect(Collectors.toList());
    }

    private int getRequiredMemoryForNeuronSeparationByObjective(String objective) {
        if (StringUtils.isBlank(objective)) {
            return 96;
        }
        switch(objective) {
            case "63x":
                return 96;
            case "40x":
                return 60;
            default:
                return 24;
        }
    }

    private void runAllAlignments(Number sampleId, JacsServiceData jacsServiceData, String sampleDataRootDir, List<String> alignmentAlgorithms, JacsServiceResult<List<SampleProcessorResult>> lspr, List<JacsServiceResult<NeuronSeparationFiles>> neuronSeparationsResults) {
        Map<NeuronSeparationFiles, JacsServiceResult<NeuronSeparationFiles>> indexedNeuronSeparationResults = Maps.uniqueIndex(neuronSeparationsResults,
                new Function<JacsServiceResult<NeuronSeparationFiles>, NeuronSeparationFiles>() {
                    @Nullable
                    @Override
                    public NeuronSeparationFiles apply(JacsServiceResult<NeuronSeparationFiles> nsr) {
                        return nsr.getResult();
                    }
                });

        List<NeuronSeparationFiles> sampleNeuronSeparationFiles =  neuronSeparationsResults.stream().map(JacsServiceResult::getResult).collect(Collectors.toList());

        Sample sample = sampleDataService.getSampleById(jacsServiceData.getOwner(), sampleId);
        for (String alignmentAlgorithm : alignmentAlgorithms) {
            AlignmentServiceBuilder alignmentServiceBuilder = alignmentServiceBuilderFactory.getServiceArgBuilder(alignmentAlgorithm);
            if (alignmentServiceBuilder == null) {
                // unsupported alignment algorithm
                continue;
            }
            List<AlignmentServiceParams> alignmentServicesParams =
                    alignmentServiceBuilder.getAlignmentServicesArgs(
                            sample,
                            alignmentAlgorithm,
                            sampleDataRootDir,
                            lspr.getResult(),
                            sampleNeuronSeparationFiles);

            alignmentServicesParams.forEach(alignmentServiceParams -> {
                JacsServiceData neuronSeparationService = indexedNeuronSeparationResults.get(alignmentServiceParams.getNeuronSeparationFiles()).getJacsServiceData();
                runAlignment(jacsServiceData, alignmentServiceParams, lspr.getJacsServiceData(), neuronSeparationService)
                        .thenCompose((JacsServiceResult<AlignmentResult> alignmentResult) -> runAlignmentNeuronSeparation(
                                jacsServiceData,
                                alignmentServiceParams.getSampleProcessorResult(),
                                sampleDataRootDir,
                                alignmentResult.getResult().getAlignmentResultId(),
                                alignmentServiceParams.getNeuronSeparationFiles().getConsolidatedLabelPath(),
                                alignmentResult.getJacsServiceData()));
            });
        }
    }

    private ServiceComputation<JacsServiceResult<AlignmentResult>> runAlignment(JacsServiceData jacsServiceData, AlignmentServiceParams alignmentServiceParams, JacsServiceData... deps) {
        return alignmentProcessor.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Align sample")
                        .addResources(alignmentServiceParams.getResources())
                        .waitFor(deps)
                        .build(),
                alignmentServiceParams.getAlignmentServiceArgsArray()
        ).thenCompose((JacsServiceResult<AlignmentResultFiles> alignmentResultFiles) -> updateAlignmentResultsProcessor.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Update alignment results")
                        .waitFor(alignmentResultFiles.getJacsServiceData())
                        .build(),
                new ServiceArg("-sampleId", alignmentServiceParams.getSampleProcessorResult().getSampleId()),
                new ServiceArg("-objective", alignmentServiceParams.getSampleProcessorResult().getObjective()),
                new ServiceArg("-area", alignmentServiceParams.getSampleProcessorResult().getArea()),
                new ServiceArg("-runId", alignmentServiceParams.getSampleProcessorResult().getRunId()),
                new ServiceArg("-alignmentServiceId", alignmentResultFiles.getJacsServiceData().getId())
        ));
    }

    private ServiceComputation<JacsServiceResult<NeuronSeparationFiles>> runAlignmentNeuronSeparation(JacsServiceData jacsServiceData,
                                                                                                      SampleProcessorResult sampleProcessorResult,
                                                                                                      String sampleDataRootDir,
                                                                                                      Number alignmentResultId,
                                                                                                      Path consolidatedLabelFile,
                                                                                                      JacsServiceData... deps) {
        Path neuronSeparationOutputDir = Paths.get(sampleDataRootDir).resolve(FileUtils.getDataPath("Separation", alignmentResultId));
        Path previousNeuronsResult = getPreviousAlignmentBasedNeuronsResultFile(
                jacsServiceData, sampleProcessorResult.getSampleId(),
                sampleProcessorResult.getObjective(),
                sampleProcessorResult.getArea(),
                sampleProcessorResult.getRunId(),
                alignmentResultId);
        return sampleNeuronWarpingProcessor.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Warp sample neurons")
                        .addRequiredMemoryInGB(getRequiredMemoryForNeuronSeparationByObjective(sampleProcessorResult.getObjective()))
                        .waitFor(deps)
                        .build(),
                new ServiceArg("-sampleId", sampleProcessorResult.getSampleId()),
                new ServiceArg("-objective", sampleProcessorResult.getObjective()),
                new ServiceArg("-runId", sampleProcessorResult.getRunId()),
                new ServiceArg("-resultId", alignmentResultId),
                new ServiceArg("-inputFile", sampleProcessorResult.getAreaFile()),
                new ServiceArg("-outputDir", neuronSeparationOutputDir.toString()),
                new ServiceArg("-signalChannels", sampleProcessorResult.getSignalChannels()),
                new ServiceArg("-referenceChannel", sampleProcessorResult.getReferenceChannel()),
                new ServiceArg("-consolidatedLabelFile", consolidatedLabelFile.toString()),
                new ServiceArg("-previousResultFile", previousNeuronsResult != null ? previousNeuronsResult.toString() : "")
            );
    }

    private FlylightSampleArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(jacsServiceData.getArgsArray(), new FlylightSampleArgs());
    }

    private Path getNeuronSeparationOutputDir(String sampleDataRootDir, String topSubDir, Number parentResultId, String area, int resultIndex) {
        Path neuronSeparationOutputDir;
        if (StringUtils.isNotBlank(area)) {
            neuronSeparationOutputDir = Paths.get(sampleDataRootDir)
                    .resolve(FileUtils.getDataPath(topSubDir, parentResultId))
                    .resolve(area);
        } else {
            neuronSeparationOutputDir = Paths.get(sampleDataRootDir)
                    .resolve(FileUtils.getDataPath(topSubDir, parentResultId))
                    .resolve("area" + String.valueOf(resultIndex + 1));
        }
        return neuronSeparationOutputDir;
    }

    private Path getPreviousSampleProcessingBasedNeuronsResultFile(JacsServiceData jacsServiceData, Number sampleId, String objective, String area, Number runId) {
        // check previus neuron separation results and return corresponding result file
        Sample sample = sampleDataService.getSampleById(jacsServiceData.getOwner(), sampleId);
        return sample.lookupObjective(objective)
                .flatMap(objectiveSample -> objectiveSample.findPipelineRunById(runId))
                .map(IndexedReference::getReference)
                .map(sampleRun -> sampleRun.streamResults()
                        .map(IndexedReference::getReference)
                        .filter(result -> result instanceof NeuronSeparation) // only look at neuronseparation result types
                        .filter(result -> result.getParentResult() instanceof SampleProcessingResult && ((SampleProcessingResult) result.getParentResult()).getAnatomicalArea().equals(area))
                        .sorted((pr1, pr2) -> pr2.getCreationDate().compareTo(pr1.getCreationDate())) // sort by creation date desc
                        .filter(ns -> StringUtils.isNotBlank(ns.getFileName(FileType.NeuronSeparatorResult)))
                        .map(ns -> ns.getFullFilePath(FileType.NeuronSeparatorResult))
                        .findFirst()
                        .orElse(null))
                .orElse(null);
    }

    private Path getPreviousAlignmentBasedNeuronsResultFile(JacsServiceData jacsServiceData, Number sampleId, String objective, String area, Number runId, Number alignmentResultId) {
        // check previus neuron separation results and return corresponding result file
        Sample sample = sampleDataService.getSampleById(jacsServiceData.getOwner(), sampleId);
        return sample.lookupObjective(objective)
                .flatMap(objectiveSample -> objectiveSample.findPipelineRunById(runId))
                .map(IndexedReference::getReference)
                .map(sampleRun -> sampleRun.streamResults()
                        .map(IndexedReference::getReference)
                        .filter(result -> result instanceof NeuronSeparation) // only look at neuronseparation result types
                        .filter(result -> result.getParentResult() instanceof SampleAlignmentResult &&
                                ((SampleAlignmentResult) result.getParentResult()).getAnatomicalArea().equals(area) &&
                                result.getParentResult().sameId(alignmentResultId))
                        .filter(ns -> StringUtils.isNotBlank(ns.getFileName(FileType.NeuronSeparatorResult)))
                        .map(ns -> ns.getFullFilePath(FileType.NeuronSeparatorResult))
                        .findFirst()
                        .orElse(null))
                .orElse(null);
    }
}

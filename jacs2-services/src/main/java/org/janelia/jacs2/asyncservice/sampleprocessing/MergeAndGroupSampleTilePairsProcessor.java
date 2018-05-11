package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.model.domain.enums.algorithms.MergeAlgorithm;
import org.janelia.model.jacs2.domain.enums.FileType;
import org.janelia.model.jacs2.domain.sample.AnatomicalArea;
import org.janelia.model.jacs2.domain.sample.LSMImage;
import org.janelia.model.jacs2.domain.sample.TileLsmPair;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.fileservices.LinkDataProcessor;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dChannelMapProcessor;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dStitchGroupingProcessor;
import org.janelia.jacs2.asyncservice.imageservices.tools.ChannelComponents;
import org.janelia.jacs2.asyncservice.imageservices.tools.LSMProcessingTools;
import org.janelia.jacs2.asyncservice.lsmfileservices.MergeLsmPairProcessor;
import org.janelia.jacs2.asyncservice.sampleprocessing.zeiss.LSMChannel;
import org.janelia.jacs2.asyncservice.sampleprocessing.zeiss.LSMDetectionChannel;
import org.janelia.jacs2.asyncservice.sampleprocessing.zeiss.LSMMetadata;
import org.janelia.jacs2.asyncservice.utils.DataHolder;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.model.service.RegisteredJacsNotification;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Merge and group sample tile pairs  (see jacsV1 Vaa3DConvertToSampleImageService + jacsV1 Vaa3dStichAndGroupingService).
 */
@Named("mergeAndGroupSampleTilePairs")
public class MergeAndGroupSampleTilePairsProcessor extends AbstractBasicLifeCycleServiceProcessor<List<SampleAreaResult>, MergeAndGroupSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult> {

    static class MergeSampleTilePairsIntermediateResult extends GetSampleLsmsIntermediateResult {

        private String sampleName;
        private List<MergedAndGroupedAreaTiles> areasResults;

        MergeSampleTilePairsIntermediateResult(Number sampleLsmsServiceDataId) {
            super(sampleLsmsServiceDataId);
        }

        public List<MergedAndGroupedAreaTiles> getAreasResults() {
            return areasResults;
        }

        public void setAreasResults(List<MergedAndGroupedAreaTiles> areasResults) {
            this.areasResults = areasResults;
        }
    }

    private static class MergeChannelsData {
        TileLsmPair tilePair;
        List<String> unmergedInputChannels;
        List<String> mergedInputChannels;
        List<String> outputChannels;
        List<String> outputColors;
        String mapping;
        String resultsParentDir;
        String mergeTileRelativeSubDir;
        String mergeTileFile;
        String imageSize;
        String opticalResolution;
        JacsServiceData mergeTileServiceData;

        MergeChannelsData(TileLsmPair tilePair, List<String> unmergedInputChannels, List<String> mergedInputChannels, List<String> outputChannels, List<String> outputColors) {
            this.tilePair = tilePair;
            this.unmergedInputChannels = unmergedInputChannels;
            this.mergedInputChannels = mergedInputChannels;
            this.outputChannels = outputChannels;
            this.outputColors = outputColors;
            this.mapping = LSMProcessingTools.generateChannelMapping(mergedInputChannels, outputChannels);
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    static class MergedAndGroupedAreaTiles {
        Number sampleId;
        String sampleName;
        String sampleEffector;
        String objective;
        String anatomicalArea;
        String areaChannelMapping;
        ChannelComponents areaChannelComponents;
        String resultsDir;
        String mergeSubDir;
        String groupSubDir;
        List<MergeTilePairResult> mergeResults = new LinkedList<>();
        List<MergeTilePairResult> groupedTiles = new LinkedList<>();
        List<JacsServiceData> mergeTileServiceList = new LinkedList<>();
        JacsServiceData groupService;
    }

    static class ConvertTileToImageArgs extends SampleServiceArgs {
        @Parameter(names = "-mergeAlgorithm", description = "Merge algorithm", required = false)
        String mergeAlgorithm = MergeAlgorithm.FLYLIGHT.getName();
        @Parameter(names = "-channelDyeSpec", description = "Channel dye spec", required = false)
        String channelDyeSpec;
        @Parameter(names = "-outputChannelOrder", description = "Output channel order", required = false)
        String outputChannelOrder;
    }

    private static final String MERGE_DIRNAME = "merge";
    private static final String GROUP_DIRNAME = "group";

    private final UpdateSampleLSMMetadataProcessor updateSampleLSMMetadataProcessor;
    private final MergeLsmPairProcessor mergeLsmPairProcessor;
    private final Vaa3dChannelMapProcessor vaa3dChannelMapProcessor;
    private final LinkDataProcessor linkDataProcessor;
    private final Vaa3dStitchGroupingProcessor vaa3dStitchGroupingProcessor;
    private final SampleDataService sampleDataService;
    private final TimebasedIdentifierGenerator idGenerator;

    @Inject
    MergeAndGroupSampleTilePairsProcessor(ServiceComputationFactory computationFactory,
                                          JacsServiceDataPersistence jacsServiceDataPersistence,
                                          @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                          UpdateSampleLSMMetadataProcessor updateSampleLSMMetadataProcessor,
                                          MergeLsmPairProcessor mergeLsmPairProcessor,
                                          Vaa3dChannelMapProcessor vaa3dChannelMapProcessor,
                                          LinkDataProcessor linkDataProcessor,
                                          Vaa3dStitchGroupingProcessor vaa3dStitchGroupingProcessor,
                                          SampleDataService sampleDataService,
                                          @JacsDefault TimebasedIdentifierGenerator idGenerator,
                                          Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.updateSampleLSMMetadataProcessor = updateSampleLSMMetadataProcessor;
        this.mergeLsmPairProcessor = mergeLsmPairProcessor;
        this.vaa3dChannelMapProcessor = vaa3dChannelMapProcessor;
        this.linkDataProcessor = linkDataProcessor;
        this.vaa3dStitchGroupingProcessor = vaa3dStitchGroupingProcessor;
        this.sampleDataService = sampleDataService;
        this.idGenerator = idGenerator;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(MergeAndGroupSampleTilePairsProcessor.class, new ConvertTileToImageArgs());
    }

    @Override
    public ServiceResultHandler<List<SampleAreaResult>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<SampleAreaResult>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public List<SampleAreaResult> collectResult(JacsServiceResult<?> depResults) {
                MergeSampleTilePairsIntermediateResult result = (MergeSampleTilePairsIntermediateResult) depResults.getResult();
                return result.getAreasResults().stream()
                        .map((MergedAndGroupedAreaTiles tmpAreaResult) -> {
                            SampleAreaResult areaResult = new SampleAreaResult();
                            areaResult.setSampleId(tmpAreaResult.sampleId);
                            areaResult.setSampleName(tmpAreaResult.sampleName);
                            areaResult.setSampleEffector(tmpAreaResult.sampleEffector);
                            areaResult.setObjective(tmpAreaResult.objective);
                            areaResult.setAnatomicalArea(tmpAreaResult.anatomicalArea);
                            areaResult.setResultDir(tmpAreaResult.resultsDir);
                            areaResult.setMergeRelativeSubDir(tmpAreaResult.mergeSubDir);
                            areaResult.setGroupRelativeSubDir(tmpAreaResult.groupSubDir);
                            areaResult.setConsensusChannelMapping(tmpAreaResult.areaChannelMapping);
                            areaResult.setConsensusChannelComponents(tmpAreaResult.areaChannelComponents);
                            areaResult.setMergeResults(tmpAreaResult.mergeResults);
                            areaResult.setGroupResults(tmpAreaResult.groupedTiles);
                            return areaResult;
                        })
                        .collect(Collectors.toList());
            }

            public List<SampleAreaResult> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SampleAreaResult>>() {});
            }
        };
    }

    @Override
    protected JacsServiceResult<MergeSampleTilePairsIntermediateResult> submitServiceDependencies(JacsServiceData jacsServiceData) {
        ConvertTileToImageArgs args = getArgs(jacsServiceData);

        JacsServiceData updateSampleLSMMetadataServiceRef = updateSampleLSMMetadataProcessor.createServiceData(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .build(),
                new ServiceArg("-sampleId", args.sampleId.toString()),
                new ServiceArg("-objective", args.sampleObjective),
                new ServiceArg("-area", args.sampleArea),
                new ServiceArg("-channelDyeSpec", args.channelDyeSpec),
                new ServiceArg("-sampleDataRootDir", args.sampleDataRootDir),
                new ServiceArg("-sampleLsmsSubDir", args.sampleLsmsSubDir),
                new ServiceArg("-sampleSummarySubDir", args.sampleSummarySubDir)
        );
        JacsServiceData sampleLsmsMetadataService = submitDependencyIfNotFound(updateSampleLSMMetadataServiceRef);

        // the LSM metadata is required for generating the merge tile commands so I am waiting until update completes
        return computationFactory.newCompletedComputation(sampleLsmsMetadataService)
                .thenSuspendUntil(this.suspendCondition(jacsServiceData))
                .thenApply(lsmMD -> {
                    if (lsmMD.hasCompletedUnsuccessfully()) {
                        logger.error("Abandon the rest of the merge process because it could not generate/update sample LSMs metadata");
                        throw new ComputationException(jacsServiceData, "LSM metadata is required for tile merge");
                    }
                    List<MergedAndGroupedAreaTiles> mergedAndGroupedAreas = mergeAndGroupAllTilePairs(jacsServiceData, sampleLsmsMetadataService);
                    MergeSampleTilePairsIntermediateResult result = new MergeSampleTilePairsIntermediateResult(sampleLsmsMetadataService.getId());
                    result.setAreasResults(mergedAndGroupedAreas);
                    return new JacsServiceResult<>(jacsServiceData, result);
                })
                .get()
                ;
    }

    private List<MergedAndGroupedAreaTiles> mergeAndGroupAllTilePairs(JacsServiceData jacsServiceData, JacsServiceData sampleLsmsMetadataService) {
        ConvertTileToImageArgs args = getArgs(jacsServiceData);
        List<AnatomicalArea> anatomicalAreas =
                sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(jacsServiceData.getOwnerKey(), args.sampleId, args.sampleObjective, args.sampleArea);
        BiFunction<AnatomicalArea, TileLsmPair, MergeChannelsData> channelMappingFunc;
        MergeAlgorithm mergeAlgorithm = getMergeAlgorithm(args.mergeAlgorithm);
        String multiscanBlendVersion = (mergeAlgorithm == MergeAlgorithm.FLYLIGHT_ORDERED) ? "2" : null;

        if (StringUtils.isNotBlank(args.channelDyeSpec) && StringUtils.isNotBlank(args.outputChannelOrder)) {
            // if it uses the channel dye spec and the output channel order is specified use the dye spec to deternine the ordering
            Pair<Multimap<String, String>, Map<String, String>> channelDyesMapData = LSMProcessingTools.parseChannelDyeSpec(args.channelDyeSpec);
            List<String> outputChannels = LSMProcessingTools.parseChannelComponents(args.outputChannelOrder);
            channelMappingFunc = (ar, tp) -> determineChannelMappingUsingDyeSpec(tp, channelDyesMapData, outputChannels);
        } else {
            // otherwise use the channel spec and the merge algorithm
            channelMappingFunc = (ar, tp) -> determineChannelMappingsUsingChanSpec(tp, ar.getDefaultChanSpec(), mergeAlgorithm);
        }
        BinaryOperator<MergedAndGroupedAreaTiles> channelMappingConsensusCombiner = (MergedAndGroupedAreaTiles c1, MergedAndGroupedAreaTiles c2) -> {
            c1.sampleId = c2.sampleId;
            c1.sampleName = c2.sampleName;
            c1.sampleEffector = c2.sampleEffector;
            c1.objective = c2.objective;
            c1.anatomicalArea = c2.anatomicalArea;
            // compare if the two mapping are identical
            if (c1.areaChannelMapping == null) {
                c1.areaChannelMapping = c2.areaChannelMapping;
            } else if (!c1.areaChannelMapping.equals(c2.areaChannelMapping)) {
                throw new IllegalStateException("No channel mapping consesus among tiles: " + c1.areaChannelMapping + " != " + c2.areaChannelMapping);
            }
            if (c1.areaChannelComponents == null) {
                c1.areaChannelComponents = c2.areaChannelComponents;
            } else if (!c1.areaChannelComponents.equals(c2.areaChannelComponents)) {
                throw new IllegalStateException("No channel mapping consesus among tiles: " + c1.areaChannelComponents + " != " + c2.areaChannelComponents);
            }
            c1.resultsDir = c2.resultsDir;
            c1.mergeSubDir = c2.mergeSubDir;
            c1.groupSubDir = c2.groupSubDir;
            c1.mergeResults.addAll(c2.mergeResults);
            c1.mergeTileServiceList.addAll(c2.mergeTileServiceList);
            return c1;
        };

        Function<TileLsmPair, String> mergeFileNameGenerator;
        if (canUseTileNameAsMergeFile(anatomicalAreas)) {
            mergeFileNameGenerator = tp -> "tile-" + tp.getNonNullableTileName();
        } else {
            mergeFileNameGenerator = tp -> tp.getNonNullableTileName() + "tile-" + idGenerator.generateId();
        }
        return anatomicalAreas.stream()
                .map(ar -> mergeChannelsForAllTilesFromAnArea(ar, mergeAlgorithm, multiscanBlendVersion, channelMappingFunc, mergeFileNameGenerator,
                        channelMappingConsensusCombiner, jacsServiceData, args, sampleLsmsMetadataService))
                .map(atr -> {
                    // if there is more than one tile in the current area then group the tiles for stitching
                    if (atr.mergeResults.size() > 1) {
                        return groupTiles(atr, jacsServiceData);
                    } else {
                        return atr;
                    }
                })
                .collect(Collectors.toList());
    }

    private boolean canUseTileNameAsMergeFile(List<AnatomicalArea> anatomicalAreas) {
        long tilePairsCount = anatomicalAreas.stream().flatMap(ar -> ar.getTileLsmPairs().stream()).count();
        long uniqueTileNamesCount = anatomicalAreas.stream()
                .flatMap(ar -> ar.getTileLsmPairs().stream())
                .map(TileLsmPair::getTileName)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet())
                .size();
        return tilePairsCount == uniqueTileNamesCount;
    }

    private MergedAndGroupedAreaTiles mergeChannelsForAllTilesFromAnArea(AnatomicalArea ar,
                                                                         MergeAlgorithm mergeAlgorithm,
                                                                         String multiscanBlendVersion,
                                                                         BiFunction<AnatomicalArea, TileLsmPair, MergeChannelsData> channelMappingFunc,
                                                                         Function <TileLsmPair, String> mergeFileNameGenerator,
                                                                         BinaryOperator<MergedAndGroupedAreaTiles> channelMappingConsensusCombiner,
                                                                         JacsServiceData jacsServiceData,
                                                                         ConvertTileToImageArgs args,
                                                                         JacsServiceData... deps) {
        return ar.getTileLsmPairs().stream()
                .map((TileLsmPair tp) -> channelMappingFunc.apply(ar, tp)) // determine channel mapping
                .map((MergeChannelsData mcd) -> mergeChannelsForATilePair(ar, mcd, mergeAlgorithm, multiscanBlendVersion, mergeFileNameGenerator, jacsServiceData, args, deps)) // merge channels from a tile pair
                .reduce(new MergedAndGroupedAreaTiles(), (MergedAndGroupedAreaTiles ac, MergeChannelsData mcd) -> { // generate consensus result
                    MergeTilePairResult mergeResult = new MergeTilePairResult();
                    mergeResult.setTileName(mcd.tilePair.getTileName());
                    mergeResult.setImageSize(mcd.imageSize);
                    mergeResult.setOpticalResolution(mcd.opticalResolution);
                    mergeResult.setMergeResultFile(mcd.mergeTileFile);
                    mergeResult.setChannelMapping(mcd.mapping);
                    mergeResult.setChannelComponents(LSMProcessingTools.extractChannelComponents(mcd.outputChannels));
                    mergeResult.setChannelColors(mcd.outputColors);

                    MergedAndGroupedAreaTiles areaTiles = new MergedAndGroupedAreaTiles();
                    areaTiles.sampleId = ar.getSampleId();
                    areaTiles.sampleName = ar.getSampleName();
                    areaTiles.sampleEffector = ar.getSampleEffector();
                    areaTiles.objective = ar.getObjective();
                    areaTiles.anatomicalArea = ar.getName();
                    areaTiles.areaChannelMapping = LSMProcessingTools.generateOutputChannelReordering(mcd.unmergedInputChannels, mcd.outputChannels);
                    areaTiles.areaChannelComponents = mergeResult.getChannelComponents();
                    areaTiles.mergeResults.add(mergeResult);
                    areaTiles.resultsDir = mcd.resultsParentDir;
                    areaTiles.mergeSubDir = mcd.mergeTileRelativeSubDir;
                    areaTiles.groupSubDir = null; // no grouping yet
                    areaTiles.mergeTileServiceList.add(mcd.mergeTileServiceData);
                    return channelMappingConsensusCombiner.apply(ac, areaTiles);
                }, channelMappingConsensusCombiner);
    }

    private MergeChannelsData mergeChannelsForATilePair(AnatomicalArea ar,
                                                        MergeChannelsData mcd,
                                                        MergeAlgorithm mergeAlgorithm,
                                                        String multiscanBlendVersion,
                                                        Function<TileLsmPair, String> mergeFileNameGenerator,
                                                        JacsServiceData jacsServiceData,
                                                        ConvertTileToImageArgs args,
                                                        JacsServiceData... deps) {
        logger.info("Merge channel info for tile {} -> unmerged channels: {}, merged channels: {}, output: {}, mapping: {}",
                mcd.tilePair.getTileName(), mcd.unmergedInputChannels, mcd.mergedInputChannels, mcd.outputChannels, mcd.mapping);
        JacsServiceData mergeLsmPairsService;

        Path areaResultsParentDir = SampleServicesUtils.getImageDataPath(args.sampleDataRootDir, args.sampleSitchingSubDir, ar.getObjective(), ar.getName());
        String mergeSubDir = MERGE_DIRNAME;
        Path mergedResultDir = areaResultsParentDir.resolve(mergeSubDir);
        Path channelMappingInput;
        Path mergedResultFileName = FileUtils.getFilePath(mergedResultDir, mergeFileNameGenerator.apply(mcd.tilePair), ".v3draw");
        if (mcd.tilePair.hasTwoLsms()) {
            mergeLsmPairsService = mergeLsmPairProcessor.createServiceData(
                    new ServiceExecutionContext.Builder(jacsServiceData)
                            .addRequiredMemoryInGB(32)
                            .waitFor(deps)
                            .registerProcessingNotification(
                                    FlylightSampleEvents.MERGE_LSMS,
                                    jacsServiceData.getProcessingStageNotification(FlylightSampleEvents.MERGE_LSMS, new RegisteredJacsNotification().withDefaultLifecycleStages())
                                            .map(n -> n.addNotificationField("sampleId", ar.getSampleId())
                                                            .addNotificationField("sampleName", ar.getSampleName())
                                                            .addNotificationField("objective", ar.getObjective())
                                                            .addNotificationField("area", ar.getName())
                                            )
                            )
                            .build(),
                    new ServiceArg("-lsm1", SampleServicesUtils.getImageFile(args.sampleDataRootDir, args.sampleLsmsSubDir,
                            ar.getObjective(),
                            ar.getName(),
                            mcd.tilePair.getFirstLsm()).toString()),
                    new ServiceArg("-lsm2", SampleServicesUtils.getImageFile(args.sampleDataRootDir, args.sampleLsmsSubDir,
                            ar.getObjective(),
                            ar.getName(),
                            mcd.tilePair.getSecondLsm()).toString()),
                    new ServiceArg("-microscope1", mcd.tilePair.getFirstLsm().getMicroscope()),
                    new ServiceArg("-microscope2", mcd.tilePair.getSecondLsm().getMicroscope()),
                    new ServiceArg("-distortionCorrection", false),
                    new ServiceArg("-multiscanVersion", multiscanBlendVersion),
                    new ServiceArg("-outputFile", mergedResultFileName.toString())
            );
            channelMappingInput = mergedResultFileName;
            mcd.imageSize = LSMProcessingTools.reconcileValues(
                    mcd.tilePair.getFirstLsm().getImageSize(),
                    mcd.tilePair.getSecondLsm().getImageSize());
            mcd.opticalResolution = LSMProcessingTools.reconcileValues(
                    mcd.tilePair.getFirstLsm().getOpticalResolution(),
                    mcd.tilePair.getSecondLsm().getOpticalResolution());
        } else {
            // no merge is necessary so the result is the tile's LSM but create a link in the merge directory
            channelMappingInput = FileUtils.getFilePath(
                    mergedResultDir,
                    SampleServicesUtils.getImageFile(args.sampleDataRootDir, args.sampleLsmsSubDir,
                            ar.getObjective(),
                            ar.getName(),
                            mcd.tilePair.getFirstLsm()).toString());
            mergeLsmPairsService = linkDataProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                            .waitFor(deps)
                            .build(),
                    new ServiceArg("-source", SampleServicesUtils.getImageFile(args.sampleDataRootDir, args.sampleLsmsSubDir,
                            ar.getObjective(),
                            ar.getName(),
                            mcd.tilePair.getFirstLsm()).toString()),
                    new ServiceArg("-target", channelMappingInput.toString())
            );
            mcd.imageSize = mcd.tilePair.getFirstLsm().getImageSize();
            mcd.opticalResolution = mcd.tilePair.getFirstLsm().getOpticalResolution();
        }
        mergeLsmPairsService = submitDependencyIfNotFound(mergeLsmPairsService);
        logger.info("Map channels {} + {} -> {}", mergedResultFileName, mcd.mapping, mergedResultFileName);
        // since the channels were in the right order no re-ordering of the channels is necessary
        JacsServiceData mapChannelsService = vaa3dChannelMapProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .waitFor(mergeLsmPairsService)
                        .registerProcessingNotification(
                                FlylightSampleEvents.MAP_CHANNELS,
                                new RegisteredJacsNotification()
                                        .withDefaultLifecycleStages()
                                        .addNotificationField("sampleId", ar.getSampleId())
                                        .addNotificationField("sampleName", ar.getSampleName())
                                        .addNotificationField("objective", ar.getObjective())
                                        .addNotificationField("area", ar.getName())
                                        .addNotificationField("mapping", mcd.mapping)
                        )
                        .build(),
                new ServiceArg("-inputFile", channelMappingInput.toString()),
                new ServiceArg("-outputFile", mergedResultFileName.toString()),
                new ServiceArg("-channelMapping", mcd.mapping)
        );
        mcd.mergeTileServiceData = submitDependencyIfNotFound(mapChannelsService);
        mcd.resultsParentDir = areaResultsParentDir.toString();
        mcd.mergeTileRelativeSubDir = mergeSubDir;
        mcd.mergeTileFile = mergedResultFileName.toString();
        return mcd;
    }

    private MergedAndGroupedAreaTiles groupTiles(MergedAndGroupedAreaTiles mergeTileResults, JacsServiceData jacsServiceData) {
        Path resultsDir = Paths.get(mergeTileResults.resultsDir);
        Path mergeDir = resultsDir.resolve(mergeTileResults.mergeSubDir);
        String groupSubDir = GROUP_DIRNAME;
        Path groupDir = resultsDir.resolve(groupSubDir);
        String referenceChannelNumber = mergeTileResults.areaChannelComponents.referenceChannelNumbers;
        JacsServiceData groupingService = vaa3dStitchGroupingProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Group tiles")
                        .addRequiredMemoryInGB(72)
                        .waitFor(mergeTileResults.mergeTileServiceList.toArray(new JacsServiceData[mergeTileResults.mergeTileServiceList.size()]))
                        .build(),
                new ServiceArg("-inputDir", mergeDir.toString()),
                new ServiceArg("-outputDir", groupDir.toString()),
                new ServiceArg("-refchannel", referenceChannelNumber)
        );
        mergeTileResults.groupService = submitDependencyIfNotFound(groupingService);
        mergeTileResults.groupSubDir = groupSubDir;
        return mergeTileResults;
    }

    @Override
    protected ServiceComputation<JacsServiceResult<MergeSampleTilePairsIntermediateResult>> processing(JacsServiceResult<MergeSampleTilePairsIntermediateResult> depResults) {
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    pd.getResult().getAreasResults().stream()
                            .forEach(areaResult -> {
                                if (areaResult.groupService != null) {
                                    this.groupTilesForStitching(areaResult);
                                } else {
                                    areaResult.mergeResults.forEach(tp -> areaResult.groupedTiles.add(tp));
                                }
                            })
                            ;
                    return pd;
                });
    }

    private void groupTilesForStitching(MergedAndGroupedAreaTiles areaTiles) {
        JacsServiceData groupServiceData = jacsServiceDataPersistence.findById(areaTiles.groupService.getId());
        File groupsFile = vaa3dStitchGroupingProcessor.getResultHandler().getServiceDataResult(groupServiceData);
        List<List<String>> tileGroups = readGroups(groupsFile.toPath());
        Set<String> largestTileGroup = ImmutableSet.copyOf(
                tileGroups.stream()
                        .max((l1, l2) -> CollectionUtils.size(l1) - CollectionUtils.size(l2))
                        .orElseGet(() -> {
                            logger.warn("Non non empty group found among {} from {}", tileGroups, groupsFile);
                            return ImmutableList.of();
                        })
        );
        List<MergeTilePairResult> tilesFromLargestGroup = areaTiles.mergeResults.stream()
                .filter(mt -> largestTileGroup.contains(mt.getMergeResultFile()))
                .collect(Collectors.toList());

        tilesFromLargestGroup.stream()
                .forEach(tp -> {
                    try {
                        Path tileMergedFile = Paths.get(tp.getMergeResultFile());
                        Path tileMergedFileLink = FileUtils.getFilePath(Paths.get(areaTiles.resultsDir, areaTiles.groupSubDir), tp.getMergeResultFile());
                        if (!tileMergedFile.toAbsolutePath().startsWith(tileMergedFileLink.toAbsolutePath())) {
                            Files.createSymbolicLink(tileMergedFileLink, tileMergedFile);
                        }
                        MergeTilePairResult newTilePair = new MergeTilePairResult();
                        newTilePair.setTileName(tp.getTileName());
                        newTilePair.setMergeResultFile(tileMergedFileLink.toString());
                        newTilePair.setChannelMapping(tp.getChannelMapping());
                        newTilePair.setChannelComponents(tp.getChannelComponents());
                        areaTiles.groupedTiles.add(newTilePair);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    private List<List<String>> readGroups(Path groupsFile) {
        try {
            List<String> groupsFileContent = Files.readAllLines(groupsFile);
            List<List<String>> groups = new ArrayList<>();
            List<String> currGroup = new ArrayList<>();
            groupsFileContent.stream()
                    .map(String::trim)
                    .filter(StringUtils::isNotEmpty)
                    .forEach(l -> {
                        if (l.startsWith("# tiled image group")) {
                            if (!currGroup.isEmpty()) {
                                // copy the elements from the current group to a new list and add it to the groups
                                groups.add(new ArrayList<>(currGroup));
                                currGroup.clear();
                            }
                        } else {
                            currGroup.add(l);
                        }
                    });
            if (!currGroup.isEmpty()) {
                groups.add(currGroup);
            }
            return groups;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }

    private ConvertTileToImageArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new ConvertTileToImageArgs());
    }

    private MergeChannelsData determineChannelMappingUsingDyeSpec(TileLsmPair tilePair,
                                                                  Pair<Multimap<String, String>, Map<String, String>> channelDyesMapData,
                                                                  List<String> outputChannels) {
        logger.info("Determine channel mapping using dye spec for {}", tilePair);
        Multimap<String, String> channelTagToDyesMapping = channelDyesMapData.getLeft();
        Collection<String> referenceDyes = channelTagToDyesMapping.get("reference");

        Map<String, String> dyesToTagMap = channelDyesMapData.getRight();
        Map<String, String> dyesToColorMap = new HashMap<>();

        DataHolder<String> referenceDye = new DataHolder<>();
        LSMImage lsm1 = tilePair.getFirstLsm();
        LSMMetadata lsm1Metadata = LSMProcessingTools.getLSMMetadata(lsm1.getFileName(FileType.LsmMetadata));
        List<String> lsm1DyeArray = new ArrayList<>();
        List<String> lsm2DyeArray = new ArrayList<>();
        Map<String, String> lsm2DyeToColorMap = new HashMap<>();
        List<String> mergedDyeArray = new ArrayList<>();
        collectDyes(lsm1Metadata, referenceDyes, referenceDye, mergedDyeArray, lsm1DyeArray, dyesToColorMap);

        LSMImage lsm2 = tilePair.getSecondLsm();
        if (lsm2 != null) {
            LSMMetadata lsm2Metadata = LSMProcessingTools.getLSMMetadata(lsm2.getFileName(FileType.LsmMetadata));
            collectDyes(lsm2Metadata, referenceDyes, referenceDye, mergedDyeArray, lsm2DyeArray, dyesToColorMap);
            // Add the reference dye back in, at the end
            if (referenceDye.isPresent()) {
                mergedDyeArray.add(referenceDye.getData());
            } else {
                throw new IllegalArgumentException("No reference dye has been specified");
            }
        }
        List<String> unmergedChannelList = new ArrayList<>();

        unmergedChannelList.addAll(convertDyesToChannelTags(lsm1DyeArray, dyesToTagMap));
        unmergedChannelList.addAll(convertDyesToChannelTags(lsm2DyeArray, dyesToTagMap));

        return new MergeChannelsData(tilePair,
                unmergedChannelList,
                ImmutableList.copyOf(convertDyesToChannelTags(mergedDyeArray, dyesToTagMap)),
                outputChannels,
                outputChannels.stream()
                        .filter(chTag -> channelTagToDyesMapping.containsKey(chTag))
                        .map(chTag -> {
                            if (channelTagToDyesMapping.containsKey(chTag)) {
                                return channelTagToDyesMapping.get(chTag).stream().findFirst().orElse("");
                            } else {
                                return "";
                            }
                        })
                        .collect(Collectors.toList()));
    }

    private MergeChannelsData determineChannelMappingsUsingChanSpec(TileLsmPair tilePair, String outputChannelSpec, MergeAlgorithm mergeAlgorithm) {
        logger.info("Determine channel mapping using channel spec for {}", tilePair);
        LSMImage lsm1 = tilePair.getFirstLsm();
        LSMMetadata lsm1Metadata = LSMProcessingTools.getLSMMetadata(lsm1.getFileName(FileType.LsmMetadata));
        LSMImage lsm2 = tilePair.getSecondLsm();
        MergeChannelsData channelMapping;
        if (lsm2 != null) {
            LSMMetadata lsm2Metadata = LSMProcessingTools.getLSMMetadata(lsm2.getFileName(FileType.LsmMetadata));
            // concatenate the channels from the two LSMs
            switch (mergeAlgorithm) {
                case FLYLIGHT_ORDERED:
                    channelMapping = mergeUsingFlylightOrderedAlgorithm(tilePair, lsm1, lsm1Metadata, lsm2, lsm2Metadata, outputChannelSpec);
                    break;
                default: // FLYLIGHT
                    channelMapping = mergeUsingFlylightAlgorithm(tilePair, lsm1, lsm1Metadata, lsm2, lsm2Metadata);
                    break;
            }
        } else {
            int refIndex1 = LSMProcessingTools.getOneBasedRefChannelIndex(lsm1Metadata);
            String chanSpec1 = StringUtils.defaultIfBlank(lsm1.getChanSpec(),
                    LSMProcessingTools.createChanSpec(lsm1.getNumChannels(), refIndex1));
            channelMapping = createInputAndOutputChannels(tilePair, chanSpec1, outputChannelSpec, LSMProcessingTools.parseChannelComponents(lsm1.getChannelColors()));
        }
        return channelMapping;
    }

    private MergeChannelsData createInputAndOutputChannels(TileLsmPair tilePair, String inputChanSpec, String outputChanSpec, List<String> outputChannelColors) {
        List<String> inputChannelList = LSMProcessingTools.convertChanSpecToList(inputChanSpec);
        List<String> inputChannelWithSingleRefList = LSMProcessingTools.convertChanSpecToList(moveRefChannelAtTheEnd(inputChanSpec));
        List<String> outputChannelList;
        if (StringUtils.isBlank(outputChanSpec)) {
            // move the reference channel at the end
            outputChannelList = inputChannelList;
        } else {
            outputChannelList = LSMProcessingTools.convertChanSpecToList(outputChanSpec);
        }
        return new MergeChannelsData(tilePair, inputChannelList, inputChannelWithSingleRefList, outputChannelList, outputChannelColors);
    }

    private MergeChannelsData mergeUsingFlylightOrderedAlgorithm(TileLsmPair tilePair,
                                                                 LSMImage lsm1, LSMMetadata lsm1Metadata,
                                                                 LSMImage lsm2, LSMMetadata lsm2Metadata,
                                                                 String outputChanSpec) {

        int refIndex1 = LSMProcessingTools.getOneBasedRefChannelIndex(lsm1Metadata);
        String chanSpec1 = StringUtils.defaultIfBlank(lsm1.getChanSpec(),
                LSMProcessingTools.createChanSpec(lsm1.getNumChannels(), refIndex1));
        List<String> lsm1Colors;
        if (StringUtils.isBlank(lsm1.getChannelColors())) {
            lsm1Colors = LSMProcessingTools.getLsmChannelColors(lsm1Metadata);
        } else {
            lsm1Colors = LSMProcessingTools.parseChannelComponents(lsm1.getChannelColors());
        }
        int refIndex2 = LSMProcessingTools.getOneBasedRefChannelIndex(lsm2Metadata);
        String chanSpec2 = StringUtils.defaultIfBlank(lsm2.getChanSpec(),
                LSMProcessingTools.createChanSpec(lsm2.getNumChannels(), refIndex2));
        List<String> lsm2Colors;
        if (StringUtils.isBlank(lsm2.getChannelColors())) {
            lsm2Colors = LSMProcessingTools.getLsmChannelColors(lsm2Metadata);
        } else {
            lsm2Colors = LSMProcessingTools.parseChannelComponents(lsm2.getChannelColors());
        }
        Iterator<String> outputChanSpecItr = LSMProcessingTools.convertChanSpecToList(outputChanSpec).iterator();
        List<String> outputChanColors = Stream.concat(
                lsm1Colors.stream().filter(c -> !c.equalsIgnoreCase(LSMProcessingTools.REFERENCE_COLOR)),
                Stream.concat(
                        lsm2Colors.stream().filter(c -> !c.equalsIgnoreCase(LSMProcessingTools.REFERENCE_COLOR)),
                        Stream.of(LSMProcessingTools.REFERENCE_COLOR))
        )
                .filter(chColor -> outputChanSpecItr.hasNext())
                .flatMap(chColor -> {
                    String chnSpec = outputChanSpecItr.next();
                    if (LSMProcessingTools.isReferenceChanSpec(chnSpec)) {
                        if (outputChanSpecItr.hasNext()) {
                            outputChanSpecItr.next(); // consume one more char
                            return Stream.<String>of(LSMProcessingTools.REFERENCE_COLOR, chColor);
                        } else {
                            return Stream.of(LSMProcessingTools.REFERENCE_COLOR);
                        }
                    } else {
                        return Stream.of(chColor);
                    }
                })
                .collect(Collectors.toList());
        return createInputAndOutputChannels(tilePair, chanSpec1 + chanSpec2, outputChanSpec, outputChanColors);
    }

    private MergeChannelsData mergeUsingFlylightAlgorithm(TileLsmPair tilePair, LSMImage lsm1, LSMMetadata lsm1Metadata, LSMImage lsm2, LSMMetadata lsm2Metadata) {
        int refIndex1 = LSMProcessingTools.getOneBasedRefChannelIndex(lsm1Metadata);
        String chanSpec1 = StringUtils.defaultIfBlank(lsm1.getChanSpec(),
                LSMProcessingTools.createChanSpec(lsm1.getNumChannels(), refIndex1));

        int refIndex2 = LSMProcessingTools.getOneBasedRefChannelIndex(lsm2Metadata);
        String chanSpec2 = StringUtils.defaultIfBlank(lsm2.getChanSpec(),
                LSMProcessingTools.createChanSpec(lsm2.getNumChannels(), refIndex2));

        List<String> unmergedChannelTagList = LSMProcessingTools.convertChanSpecToList(chanSpec1+chanSpec2);

        Stream<LSMChannel> allChanels = Stream.concat(
                lsm1Metadata.getChannels().stream(),
                lsm2Metadata.getChannels().stream()
        );

        Iterator<String> tagIterator = unmergedChannelTagList.iterator();
        Iterator<LSMChannel> channelIterator = allChanels.iterator();

        String redTag = null;
        String greenTag = null;
        String blueTag = null;
        String refTag = null;
        for (; tagIterator.hasNext() && channelIterator.hasNext();) {
            LSMChannel lsmChannel = channelIterator.next();
            String color = lsmChannel.getColor();
            String tag = tagIterator.next();
            // check if the tag is the second reference channel and ignore it
            // or terminate if there's no tag left
            if (tag.startsWith("r") && !tag.equals("r0")) {
                // if this is a reference channel only consider the first reference channel and ignore all others
                if (LSMProcessingTools.REFERENCE_COLOR.equalsIgnoreCase(color)) {
                    // if both the tag and the color mark a reference channel advance both of them
                    continue;
                } else if (tagIterator.hasNext()) {
                    tag = tagIterator.next();
                } else {
                    break;
                }
            }
            if (LSMProcessingTools.RED_COLOR.equalsIgnoreCase(color)) {
                redTag = tag;
            } else if (LSMProcessingTools.GREEN_COLOR.equalsIgnoreCase(color)) {
                greenTag = tag;
            } else if (LSMProcessingTools.BLUE_COLOR.equalsIgnoreCase(color)) {
                blueTag = tag;
            } else if (refTag==null) {
                refTag = tag;
            }
        }
        // if not all RGB tags are set and there are tags left, try to set them from the remaining tags
        for (; tagIterator.hasNext(); ) {
            String tag = tagIterator.next();
            // check if the tag is the second reference channel and ignore it
            // or terminate if there's no tag left
            if (tag.startsWith("r") && !tag.equals("r0")) {
                // if this is a reference channel only consider the first reference channel and ignore all others
                if (tagIterator.hasNext()) {
                    tag = tagIterator.next();
                } else {
                    break;
                }
            }
            if (redTag == null) {
                redTag = tag;
                continue;
            }
            if (greenTag == null) {
                greenTag = tag;
                continue;
            }
            if (blueTag == null) {
                blueTag = tag;
            }
        }
        List<String> inputChannelList = ImmutableList.of(
                StringUtils.defaultString(redTag, ""),
                StringUtils.defaultString(greenTag, ""),
                StringUtils.defaultString(blueTag, ""),
                StringUtils.defaultString(refTag, "")
        );
        List<String> inputColorList = ImmutableList.of(
                StringUtils.isNotBlank(redTag) ? LSMProcessingTools.RED_COLOR : "",
                StringUtils.isNotBlank(greenTag) ? LSMProcessingTools.GREEN_COLOR : "",
                StringUtils.isNotBlank(blueTag) ? LSMProcessingTools.BLUE_COLOR : "",
                StringUtils.isNotBlank(refTag) ? LSMProcessingTools.REFERENCE_COLOR : ""
        );
        return new MergeChannelsData(tilePair, unmergedChannelTagList, inputChannelList, inputChannelList, inputColorList);
    }

    private String moveRefChannelAtTheEnd(String channelSpec) {
        // appends a single reference channel at the end and removes it from everywhere else
        // in case there is more than one reference channel
        return channelSpec.replaceAll("r", "")+"r";
    }

    /**
     * Convert the dyes back to tags using the mapping created during the parsing.
     * @param dyes collection of dyes
     * @param dyeToTagMap mapping of dyes to channel tags
     * @return the corresponding collection of channel tags
     */
    private Collection<String> convertDyesToChannelTags(Collection<String> dyes, Map<String, String> dyeToTagMap) {
        return dyes.stream()
                .map(dye -> StringUtils.defaultIfBlank(dyeToTagMap.get(dye), ""))
                .collect(Collectors.toList());
    }

    private MergeAlgorithm getMergeAlgorithm(String mergeAlgorithmName) {
        if (MergeAlgorithm.FLYLIGHT_ORDERED.name().equals(mergeAlgorithmName)) {
            return MergeAlgorithm.FLYLIGHT_ORDERED;
        } else {
            return MergeAlgorithm.FLYLIGHT;
        }
    }

    private void collectDyes(LSMMetadata lsmMetadata, Collection<String> referenceChannelDyes,
                             DataHolder<String> foundReferenceDye, List<String> signalDyes,
                             List<String> allDyes,
                             Map<String, String> dyeToColorMap) {
        for(LSMChannel lsmChannel : lsmMetadata.getChannels()) {
            LSMDetectionChannel detectionChannel = lsmMetadata.getDetectionChannel(lsmChannel);
            if (detectionChannel != null) {
                String dye = detectionChannel.getDyeName();
                if (referenceChannelDyes.contains(dye)) {
                    if (!foundReferenceDye.isPresent()) {
                        foundReferenceDye.setData(dye);
                    } else if (!foundReferenceDye.getData().equals(dye)) {
                        throw new IllegalArgumentException("Multiple reference dyes detected in a single image ("+foundReferenceDye.getData()+"!, "+dye+")");
                    }
                } else {
                    signalDyes.add(dye);
                }
                allDyes.add(dye);
                dyeToColorMap.put(dye, lsmChannel.getColor());
            }
        }
    }

}

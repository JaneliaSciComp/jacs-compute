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
import org.janelia.it.jacs.model.domain.codedValues.MergeAlgorithm;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.sample.AnatomicalArea;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
import org.janelia.it.jacs.model.domain.sample.TileLsmPair;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.DefaultServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceErrorChecker;
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
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
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
@Named("mergeSampleTilePairs")
public class MergeAndGroupSampleTilePairsProcessor extends AbstractBasicLifeCycleServiceProcessor<MergeAndGroupSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult, List<MergedAndGroupedAreaResult>> {

    static class MergeSampleTilePairsIntermediateResult extends GetSampleLsmsIntermediateResult {

        private List<MergedAndGroupedAreaTiles> areasResults;

        MergeSampleTilePairsIntermediateResult(Number getSampleLsmsServiceDataId) {
            super(getSampleLsmsServiceDataId);
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
        String mapping;
        String mergeTileDir;
        String mergeTileFile;
        JacsServiceData mergeTileServiceData;

        MergeChannelsData(TileLsmPair tilePair, List<String> unmergedInputChannels, List<String> mergedInputChannels, List<String> outputChannels) {
            this.tilePair = tilePair;
            this.unmergedInputChannels = unmergedInputChannels;
            this.mergedInputChannels = mergedInputChannels;
            this.outputChannels = outputChannels;
            this.mapping = LSMProcessingTools.generateChannelMapping(mergedInputChannels, outputChannels);
        }

        boolean isNonEmptyMapping() {
            return !mapping.replaceAll("([0-9]),\\1,?", "").equals("");
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    static class MergedAndGroupedAreaTiles {
        String objective;
        String anatomicalArea;
        String areaChannelMapping;
        ChannelComponents areaChannelComponents;
        String mergeDir;
        String groupDir;
        List<MergeTilePairResult> mergeResults = new LinkedList<>();
        List<MergeTilePairResult> stitchableTiles = new LinkedList<>();
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
        @Parameter(names = "-distortionCorrection", description = "If specified apply distortion correction", required = false)
        boolean applyDistortionCorrection;
    }

    private static final String MERGE_DIRNAME = "merge";
    private static final String GROUP_DIRNAME = "group";

    private final GetSampleLsmsMetadataProcessor getSampleLsmsMetadataProcessor;
    private final MergeLsmPairProcessor mergeLsmPairProcessor;
    private final Vaa3dChannelMapProcessor vaa3dChannelMapProcessor;
    private final LinkDataProcessor linkDataProcessor;
    private final Vaa3dStitchGroupingProcessor vaa3dStitchGroupingProcessor;
    private final SampleDataService sampleDataService;
    private final TimebasedIdentifierGenerator identifierGenerator;

    @Inject
    MergeAndGroupSampleTilePairsProcessor(ServiceComputationFactory computationFactory,
                                          JacsServiceDataPersistence jacsServiceDataPersistence,
                                          @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                          GetSampleLsmsMetadataProcessor getSampleLsmsMetadataProcessor,
                                          MergeLsmPairProcessor mergeLsmPairProcessor,
                                          Vaa3dChannelMapProcessor vaa3dChannelMapProcessor,
                                          LinkDataProcessor linkDataProcessor,
                                          Vaa3dStitchGroupingProcessor vaa3dStitchGroupingProcessor,
                                          SampleDataService sampleDataService,
                                          TimebasedIdentifierGenerator identifierGenerator,
                                          Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.getSampleLsmsMetadataProcessor = getSampleLsmsMetadataProcessor;
        this.mergeLsmPairProcessor = mergeLsmPairProcessor;
        this.vaa3dChannelMapProcessor = vaa3dChannelMapProcessor;
        this.linkDataProcessor = linkDataProcessor;
        this.vaa3dStitchGroupingProcessor = vaa3dStitchGroupingProcessor;
        this.sampleDataService = sampleDataService;
        this.identifierGenerator = identifierGenerator;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(this.getClass(), new ConvertTileToImageArgs());
    }

    @Override
    public ServiceResultHandler<List<MergedAndGroupedAreaResult>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<MergedAndGroupedAreaResult>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public List<MergedAndGroupedAreaResult> collectResult(JacsServiceResult<?> depResults) {
                MergeSampleTilePairsIntermediateResult result = (MergeSampleTilePairsIntermediateResult) depResults.getResult();
                return result.getAreasResults().stream()
                        .map(tmpAreaResult -> {
                            MergedAndGroupedAreaResult areaResult = new MergedAndGroupedAreaResult();
                            areaResult.setObjective(tmpAreaResult.objective);
                            areaResult.setAnatomicalArea(tmpAreaResult.anatomicalArea);
                            areaResult.setMergeDir(tmpAreaResult.mergeDir);
                            areaResult.setGroupDir(tmpAreaResult.groupDir);
                            areaResult.setConsensusChannelMapping(tmpAreaResult.areaChannelMapping);
                            areaResult.setConsensusChannelComponents(tmpAreaResult.areaChannelComponents);
                            areaResult.setMergeResults(tmpAreaResult.mergeResults);
                            areaResult.setStitchableTiles(tmpAreaResult.stitchableTiles);
                            return areaResult;
                        })
                        .collect(Collectors.toList());
            }

            public List<MergedAndGroupedAreaResult> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.stringToAny(jacsServiceData.getStringifiedResult(), new TypeReference<List<MergedAndGroupedAreaResult>>() {});
            }
        };
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return new DefaultServiceErrorChecker(logger);
    }

    @Override
    protected JacsServiceResult<MergeSampleTilePairsIntermediateResult> submitServiceDependencies(JacsServiceData jacsServiceData) {
        ConvertTileToImageArgs args = getArgs(jacsServiceData);

        JacsServiceData getSampleLsmsServiceRef = getSampleLsmsMetadataProcessor.createServiceData(new ServiceExecutionContext(jacsServiceData),
                new ServiceArg("-sampleId", args.sampleId.toString()),
                new ServiceArg("-objective", args.sampleObjective),
                new ServiceArg("-area", args.sampleArea),
                new ServiceArg("-sampleDataDir", args.sampleDataDir)
        );
        JacsServiceData getSampleLsmsService = submitDependencyIfNotPresent(jacsServiceData, getSampleLsmsServiceRef);

        List<MergedAndGroupedAreaTiles> mergedAndGroupedAreas = mergeAndGroupAllTilePairs(jacsServiceData, getSampleLsmsService);
        MergeSampleTilePairsIntermediateResult result = new MergeSampleTilePairsIntermediateResult(getSampleLsmsService.getId());
        result.setAreasResults(mergedAndGroupedAreas);
        return new JacsServiceResult<>(jacsServiceData, result);
    }

    private List<MergedAndGroupedAreaTiles> mergeAndGroupAllTilePairs(JacsServiceData jacsServiceData, JacsServiceData getSampleLsmsService) {
        ConvertTileToImageArgs args = getArgs(jacsServiceData);
        List<AnatomicalArea> anatomicalAreas =
                sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(jacsServiceData.getOwner(), args.sampleId, args.sampleObjective, args.sampleArea);
        BiFunction<AnatomicalArea, TileLsmPair, MergeChannelsData> channelMappingFunc;
        MergeAlgorithm mergeAlgorithm = getMergeAlgorithm(args.mergeAlgorithm);
        String multiscanBlendVersion = (args.applyDistortionCorrection || mergeAlgorithm == MergeAlgorithm.FLYLIGHT_ORDERED) ? "2" : null;

        if (StringUtils.isNotBlank(args.channelDyeSpec) && StringUtils.isNotBlank(args.outputChannelOrder)) {
            // if it uses the channel dye spec and the output channel order is specified use the dye spec to deternine the ordering
            Pair<Multimap<String, String>, Map<String, String>> channelDyesMapData = LSMProcessingTools.parseChannelDyeSpec(args.channelDyeSpec);
            List<String> outputChannels = LSMProcessingTools.parseChannelComponents(args.outputChannelOrder);
            channelMappingFunc = (ar, tp) -> determineChannelMappingUsingDyeSpec(tp, channelDyesMapData, outputChannels);
        } else {
            // otherwise use the channel spec and the merge algorithm
            channelMappingFunc = (ar, tp) -> determineChannelMappingsUsingChanSpec(tp, ar.getDefaultChanSpec(), mergeAlgorithm);
        }
        BinaryOperator<MergedAndGroupedAreaTiles> channelMappingConsensusCombiner = (c1, c2) -> {
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
            c1.mergeDir = c2.mergeDir;
            c1.mergeResults.addAll(c2.mergeResults);
            c1.mergeTileServiceList.addAll(c2.mergeTileServiceList);
            return c1;
        };

        Function<TileLsmPair, String> mergeFileNameGenerator;
        if (canUseTileNameAsMergeFile(anatomicalAreas)) {
            mergeFileNameGenerator = tp -> "tile-" + tp.getNonNullableTileName();
        } else {
            mergeFileNameGenerator = tp -> tp.getNonNullableTileName() + "tile-" + identifierGenerator.generateId();
        }
        return anatomicalAreas.stream()
                .map(ar -> mergeChannelsForAllTilesFromAnArea(ar, multiscanBlendVersion, channelMappingFunc, mergeFileNameGenerator,
                        channelMappingConsensusCombiner, jacsServiceData, args, getSampleLsmsService))
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
                                                                         String multiscanBlendVersion,
                                                                         BiFunction<AnatomicalArea, TileLsmPair, MergeChannelsData> channelMappingFunc,
                                                                         Function <TileLsmPair, String> mergeFileNameGenerator,
                                                                         BinaryOperator<MergedAndGroupedAreaTiles> channelMappingConsensusCombiner,
                                                                         JacsServiceData jacsServiceData,
                                                                         ConvertTileToImageArgs args,
                                                                         JacsServiceData... deps) {
        return ar.getTileLsmPairs().stream()
                .map(tp -> channelMappingFunc.apply(ar, tp))
                .map(mcd -> mergeChannelsForATilePair(ar, mcd, multiscanBlendVersion, mergeFileNameGenerator, jacsServiceData, args, deps))
                .reduce(new MergedAndGroupedAreaTiles(), (ac, mcd) -> {
                    MergeTilePairResult mergeResult = new MergeTilePairResult();
                    mergeResult.setTileName(mcd.tilePair.getTileName());
                    mergeResult.setMergeResultFile(mcd.mergeTileFile);
                    mergeResult.setChannelMapping(mcd.mapping);
                    mergeResult.setChannelComponents(LSMProcessingTools.extractChannelComponents(mcd.outputChannels));

                    MergedAndGroupedAreaTiles areaTiles = new MergedAndGroupedAreaTiles();
                    areaTiles.objective = ar.getObjective();
                    areaTiles.anatomicalArea = ar.getName();
                    areaTiles.areaChannelMapping = LSMProcessingTools.generateOutputChannelReordering(mcd.unmergedInputChannels, mcd.outputChannels);
                    areaTiles.areaChannelComponents = mergeResult.getChannelComponents();
                    areaTiles.mergeResults.add(mergeResult);
                    areaTiles.mergeDir = mcd.mergeTileDir;
                    areaTiles.mergeTileServiceList.add(mcd.mergeTileServiceData);
                    return channelMappingConsensusCombiner.apply(ac, areaTiles);
                }, channelMappingConsensusCombiner);
    }

    private MergeChannelsData mergeChannelsForATilePair(AnatomicalArea ar,
                                                        MergeChannelsData mcd,
                                                        String multiscanBlendVersion,
                                                        Function<TileLsmPair, String> mergeFileNameGenerator,
                                                        JacsServiceData jacsServiceData,
                                                        ConvertTileToImageArgs args,
                                                        JacsServiceData... deps) {
        logger.info("Merge channel info for tile {} -> unmerged channels: {}, merged channels: {}, output: {}, mapping: {}",
                mcd.tilePair.getTileName(), mcd.unmergedInputChannels, mcd.mergedInputChannels, mcd.outputChannels, mcd.mapping);
        JacsServiceData mergeLsmPairsService;
        Path mergedResultDir = FileUtils.getFilePath(
                SampleServicesUtils.getImageDataPath(args.sampleDataDir, ar.getObjective(), ar.getName()),
                MERGE_DIRNAME,
                null);
        Path mergedResultFileName = FileUtils.getFilePath(mergedResultDir, mergeFileNameGenerator.apply(mcd.tilePair), ".v3draw");
        if (mcd.tilePair.hasTwoLsms()) {
            mergeLsmPairsService = mergeLsmPairProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                            .waitFor(deps)
                            .build(),
                    new ServiceArg("-lsm1", SampleServicesUtils.getImageFile(args.sampleDataDir,
                            ar.getObjective(),
                            ar.getName(),
                            mcd.tilePair.getFirstLsm()).toString()),
                    new ServiceArg("-lsm2", SampleServicesUtils.getImageFile(args.sampleDataDir,
                            ar.getObjective(),
                            ar.getName(),
                            mcd.tilePair.getSecondLsm()).toString()),
                    new ServiceArg("-microscope1", mcd.tilePair.getFirstLsm().getMicroscope()),
                    new ServiceArg("-microscope2", mcd.tilePair.getSecondLsm().getMicroscope()),
                    new ServiceArg("-distortionCorrection", args.applyDistortionCorrection),
                    new ServiceArg("-multiscanVersion", multiscanBlendVersion),
                    new ServiceArg("-outputFile", mergedResultFileName.toString())
            );
        } else {
            // no merge is necessary so the result is the tile's LSM but create a link in the merge directory
            mergeLsmPairsService = linkDataProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                            .waitFor(deps)
                            .build(),
                    new ServiceArg("-source", SampleServicesUtils.getImageFile(args.sampleDataDir,
                            ar.getObjective(),
                            ar.getName(),
                            mcd.tilePair.getFirstLsm()).toString()),
                    new ServiceArg("-target", mergedResultFileName.toString())
            );
        }
        mergeLsmPairsService = submitDependencyIfNotPresent(jacsServiceData, mergeLsmPairsService);
        if (mcd.isNonEmptyMapping()) {
            logger.info("Map channels {} + {} -> {}", mergedResultFileName, mcd.mapping, mergedResultFileName);
            // since the channels were in the right order no re-ordering of the channels is necessary
            JacsServiceData mapChannelsService = vaa3dChannelMapProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                            .waitFor(mergeLsmPairsService)
                            .build(),
                    new ServiceArg("-inputFile", mergedResultFileName.toString()),
                    new ServiceArg("-outputFile", mergedResultFileName.toString()),
                    new ServiceArg("-channelMapping", mcd.mapping)
            );
            mcd.mergeTileServiceData = submitDependencyIfNotPresent(jacsServiceData, mapChannelsService);
        } else {
            logger.info("No mapping necessary for {} - channels were in the expected order: {}", mergedResultFileName, mcd.mapping);
            mcd.mergeTileServiceData = mergeLsmPairsService;
        }
        mcd.mergeTileDir = mergedResultDir.toString();
        mcd.mergeTileFile = mergedResultFileName.toString();
        return mcd;
    }

    private MergedAndGroupedAreaTiles groupTiles(MergedAndGroupedAreaTiles mergeTileResults, JacsServiceData jacsServiceData) {
        Path mergeDir = Paths.get(mergeTileResults.mergeDir);
        Path groupDir = mergeDir.getParent().resolve(GROUP_DIRNAME);
        String referenceChannelNumber = mergeTileResults.areaChannelComponents.referenceChannelNumbers;
        JacsServiceData groupingService = vaa3dStitchGroupingProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Group tiles")
                        .waitFor(mergeTileResults.mergeTileServiceList.toArray(new JacsServiceData[mergeTileResults.mergeTileServiceList.size()]))
                        .build(),
                new ServiceArg("-inputDir", mergeDir.toString()),
                new ServiceArg("-outputDir", groupDir.toString()),
                new ServiceArg("-refchannel", referenceChannelNumber)
        );
        mergeTileResults.groupService = submitDependencyIfNotPresent(jacsServiceData, groupingService);
        mergeTileResults.groupDir = groupDir.toAbsolutePath().toString();
        return mergeTileResults;
    }

    @Override
    protected ServiceComputation<JacsServiceResult<MergeSampleTilePairsIntermediateResult>> processing(JacsServiceResult<MergeSampleTilePairsIntermediateResult> depResults) {
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    pd.getResult().getAreasResults().stream()
                            .filter(areaResult -> areaResult.groupService != null)
                            .forEach(this::updateTilePairs)
                            ;
                    return pd;
                });
    }

    private void updateTilePairs(MergedAndGroupedAreaTiles areaTiles) {
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
                        Path tileMergedFileLink = FileUtils.getFilePath(Paths.get(areaTiles.groupDir), tp.getMergeResultFile());
                        if (!Files.isSameFile(tileMergedFile, tileMergedFileLink)) {
                            Files.createSymbolicLink(tileMergedFile, tileMergedFileLink);
                        }
                        MergeTilePairResult newTilePair = new MergeTilePairResult();
                        newTilePair.setTileName(tp.getTileName());
                        newTilePair.setMergeResultFile(tileMergedFileLink.toString());
                        newTilePair.setChannelMapping(tp.getChannelMapping());
                        newTilePair.setChannelComponents(tp.getChannelComponents());
                        areaTiles.stitchableTiles.add(newTilePair);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
    }

    private List<List<String>> readGroups(Path groupsFile) {
        try {
            List<String> groupsFileContent = Files.readAllLines(groupsFile);
            List<List<String>> groups = new ArrayList<List<String>>();
            List<String> currGroup = new ArrayList<String>();
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
        return SampleServiceArgs.parse(jacsServiceData.getArgsArray(), new ConvertTileToImageArgs());
    }

    private MergeChannelsData determineChannelMappingUsingDyeSpec(TileLsmPair tilePair,
                                                                  Pair<Multimap<String, String>, Map<String, String>> channelDyesMapData,
                                                                  List<String> outputChannels) {
        Collection<String> referenceDyes = channelDyesMapData.getLeft().get("reference");
        Map<String, String> dyesToTagMap = channelDyesMapData.getRight();

        DataHolder<String> referenceDye = new DataHolder<>();
        LSMImage lsm1 = tilePair.getFirstLsm();
        LSMMetadata lsm1Metadata = LSMProcessingTools.getLSMMetadata(lsm1.getFileName(FileType.LsmMetadata));
        List<String> lsm1DyeArray = new ArrayList<>();
        List<String> lsm2DyeArray = new ArrayList<>();
        List<String> mergedDyeArray = new ArrayList<>();
        collectDyes(lsm1Metadata, referenceDyes, referenceDye, mergedDyeArray, lsm1DyeArray);

        LSMImage lsm2 = tilePair.getSecondLsm();
        if (lsm2 != null) {
            LSMMetadata lsm2Metadata = LSMProcessingTools.getLSMMetadata(lsm2.getFileName(FileType.LsmMetadata));
            collectDyes(lsm2Metadata, referenceDyes, referenceDye, mergedDyeArray, lsm2DyeArray);
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
                outputChannels);
    }

    private MergeChannelsData determineChannelMappingsUsingChanSpec(TileLsmPair tilePair, String outputChannelSpec, MergeAlgorithm mergeAlgorithm) {
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
            channelMapping = createInputAndOutputChannels(tilePair, chanSpec1, outputChannelSpec);
        }
        return channelMapping;
    }

    private MergeChannelsData createInputAndOutputChannels(TileLsmPair tilePair, String inputChanSpec, String outputChanSpec) {
        List<String> inputChannelList = LSMProcessingTools.convertChanSpecToList(inputChanSpec);
        List<String> inputChannelWithSingleRefList = LSMProcessingTools.convertChanSpecToList(moveRefChannelAtTheEnd(inputChanSpec));
        List<String> outputChannelList;
        if (StringUtils.isBlank(outputChanSpec)) {
            // move the reference channel at the end
            outputChannelList = inputChannelList;
        } else {
            outputChannelList = LSMProcessingTools.convertChanSpecToList(outputChanSpec);
        }
        return new MergeChannelsData(tilePair, inputChannelList, inputChannelWithSingleRefList, outputChannelList);
    }

    private MergeChannelsData mergeUsingFlylightOrderedAlgorithm(TileLsmPair tilePair,
                                                                 LSMImage lsm1, LSMMetadata lsm1Metadata,
                                                                 LSMImage lsm2, LSMMetadata lsm2Metadata,
                                                                 String outputChanSpec) {

        int refIndex1 = LSMProcessingTools.getOneBasedRefChannelIndex(lsm1Metadata);
        String chanSpec1 = StringUtils.defaultIfBlank(lsm1.getChanSpec(),
                LSMProcessingTools.createChanSpec(lsm1.getNumChannels(), refIndex1));

        int refIndex2 = LSMProcessingTools.getOneBasedRefChannelIndex(lsm2Metadata);
        String chanSpec2 = StringUtils.defaultIfBlank(lsm2.getChanSpec(),
                LSMProcessingTools.createChanSpec(lsm2.getNumChannels(), refIndex2));

        return createInputAndOutputChannels(tilePair, chanSpec1 + chanSpec2, outputChanSpec);
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
                if ("#FFFFFF".equalsIgnoreCase(color)) {
                    // if both the tag and the color mark a reference channel advance both of them
                    continue;
                } else if (tagIterator.hasNext()) {
                    tag = tagIterator.next();
                } else {
                    break;
                }
            }
            if ("#FF0000".equalsIgnoreCase(color)) {
                redTag = tag;
            } else if ("#00FF00".equalsIgnoreCase(color)) {
                greenTag = tag;
            } else if ("#0000FF".equalsIgnoreCase(color)) {
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
        return new MergeChannelsData(tilePair, unmergedChannelTagList, inputChannelList, inputChannelList);
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
                .filter(dyeToTagMap::containsKey)
                .map(dyeToTagMap::get)
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
                             DataHolder<String> foundReferenceDye, Collection<String> signalDyes,
                             Collection<String> allDyes) {
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
            }
        }
    }

}

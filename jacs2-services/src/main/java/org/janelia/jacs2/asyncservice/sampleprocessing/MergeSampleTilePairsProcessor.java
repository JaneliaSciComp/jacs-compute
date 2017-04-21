package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
import org.janelia.jacs2.asyncservice.common.ServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dChannelMapProcessor;
import org.janelia.jacs2.asyncservice.imageservices.tools.ChannelComponents;
import org.janelia.jacs2.asyncservice.imageservices.tools.LSMProcessingTools;
import org.janelia.jacs2.asyncservice.lsmfileservices.MergeLsmPairProcessor;
import org.janelia.jacs2.asyncservice.sampleprocessing.zeiss.LSMChannel;
import org.janelia.jacs2.asyncservice.sampleprocessing.zeiss.LSMDetectionChannel;
import org.janelia.jacs2.asyncservice.sampleprocessing.zeiss.LSMMetadata;
import org.janelia.jacs2.asyncservice.utils.DataHolder;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Merge sample tile pairs.
 */
@Named("mergeSampleTilePairs")
public class MergeSampleTilePairsProcessor extends AbstractBasicLifeCycleServiceProcessor<MergeSampleTilePairsProcessor.MergeSampleTilePairsIntermediateResult, File> {

    static class MergeSampleTilePairsIntermediateResult extends GetSampleLsmsIntermediateResult {

        private ChannelMappingsConsesus channelMapping;

        MergeSampleTilePairsIntermediateResult(Number getSampleLsmsServiceDataId) {
            super(getSampleLsmsServiceDataId);
        }

        ChannelMappingsConsesus getChannelMapping() {
            return channelMapping;
        }

        void setChannelMapping(ChannelMappingsConsesus channelMapping) {
            this.channelMapping = channelMapping;
        }
    }

    static class MergeChannelsData {
        TileLsmPair tilePair;
        List<String> unmergedInputChannels;
        List<String> mergedInputChannels;
        List<String> outputChannels;
        String mapping;

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

    static class ChannelMappingsConsesus {
        String channelMapping;
        ChannelComponents outputChannelComponents;
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

    private final GetSampleLsmsMetadataProcessor getSampleLsmsMetadataProcessor;
    private final MergeLsmPairProcessor mergeLsmPairProcessor;
    private final Vaa3dChannelMapProcessor vaa3dChannelMapProcessor;
    private final SampleDataService sampleDataService;

    @Inject
    MergeSampleTilePairsProcessor(ServiceComputationFactory computationFactory,
                                  JacsServiceDataPersistence jacsServiceDataPersistence,
                                  @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                  GetSampleLsmsMetadataProcessor getSampleLsmsMetadataProcessor,
                                  MergeLsmPairProcessor mergeLsmPairProcessor,
                                  Vaa3dChannelMapProcessor vaa3dChannelMapProcessor,
                                  SampleDataService sampleDataService,
                                  Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.getSampleLsmsMetadataProcessor = getSampleLsmsMetadataProcessor;
        this.mergeLsmPairProcessor = mergeLsmPairProcessor;
        this.vaa3dChannelMapProcessor = vaa3dChannelMapProcessor;
        this.sampleDataService = sampleDataService;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(this.getClass(), new ConvertTileToImageArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return vaa3dChannelMapProcessor.getResultHandler();
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

        ChannelMappingsConsesus channelMapping = submitMergeChannelsForAllTilePairs(jacsServiceData, getSampleLsmsService);
        MergeSampleTilePairsIntermediateResult result = new MergeSampleTilePairsIntermediateResult(getSampleLsmsService.getId());
        result.setChannelMapping(channelMapping);
        return new JacsServiceResult<>(jacsServiceData, result);
    }

    private ChannelMappingsConsesus submitMergeChannelsForAllTilePairs(JacsServiceData jacsServiceData, JacsServiceData getSampleLsmsService) {
        ConvertTileToImageArgs args = getArgs(jacsServiceData);
        List<AnatomicalArea> anatomicalAreas =
                sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(jacsServiceData.getOwner(), args.sampleId, args.sampleObjective, args.sampleArea);
        BiFunction<AnatomicalArea, TileLsmPair, MergeChannelsData> channelMappingFunc;
        MergeAlgorithm mergeAlgorithm = getMergeAlgorithm(args.mergeAlgorithm);
        String multiscanBlendVersion = (args.applyDistortionCorrection || mergeAlgorithm == MergeAlgorithm.FLYLIGHT_ORDERED) ? "2" : null;

        if (StringUtils.isNotBlank(args.channelDyeSpec) && StringUtils.isNotBlank(args.outputChannelOrder)) {
            // if it uses the channel dye spec and the output channel order is specified use the dye spec to deternine the ordering
            Pair<Multimap<String, String>, Map<String, String>> channelDyesMapData = LSMProcessingTools.parseChannelDyeSpec(args.channelDyeSpec);
            List<String> outputChannels = Splitter.on(',').omitEmptyStrings().trimResults().splitToList(args.outputChannelOrder);
            channelMappingFunc = (ar, tp) -> determineChannelMappingUsingDyeSpec(tp, channelDyesMapData, outputChannels);
        } else {
            // otherwise use the channel spec and the merge algorithm
            channelMappingFunc = (ar, tp) -> determineChannelMappingsUsingChanSpec(tp, ar.getDefaultChanSpec(), mergeAlgorithm);
        }
        BinaryOperator<ChannelMappingsConsesus> channelMappingConsensusCombiner = (c1, c2) -> {
            // compare if the two mapping are identical
            if (c1.channelMapping == null) {
                c1.channelMapping = c2.channelMapping;
            } else if (!c1.channelMapping.equals(c2.channelMapping)) {
                throw new IllegalStateException("No channel mapping consesus among tiles: " + c1.channelMapping + " != " + c2.channelMapping);
            }
            if (c1.outputChannelComponents == null) {
                c1.outputChannelComponents = c2.outputChannelComponents;
            } else if (!c1.outputChannelComponents.equals(c2.outputChannelComponents)) {
                throw new IllegalStateException("No channel mapping consesus among tiles: " + c1.outputChannelComponents + " != " + c2.outputChannelComponents);
            }
            return c2;
        };
        return anatomicalAreas.stream()
                .map(ar -> {
                    ChannelMappingsConsesus consensus = ar.getTileLsmPairs().stream()
                            .map(tp -> channelMappingFunc.apply(ar, tp))
                            .map(mcd -> {
                                logger.info("Merge channel info for tile {} -> unmerged channels: {}, merged channels: {}, output: {}, mapping: {}",
                                        mcd.tilePair.getTileName(), mcd.unmergedInputChannels, mcd.mergedInputChannels, mcd.outputChannels, mcd.mapping);
                                JacsServiceData mergeChannelsService = null;
                                String mergedFileName;
                                if (mcd.tilePair.hasTwoLsms()) {
                                    mergedFileName = FileUtils.getFilePath(Paths.get(args.sampleDataDir), mcd.tilePair.getTileName(), "vaa3d").toString();
                                    mergeChannelsService = mergeLsmPairProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                                                    .waitFor(getSampleLsmsService)
                                                    .build(),
                                            new ServiceArg("-lsm1", SampleServicesUtils.getImageFile(args.sampleDataDir, mcd.tilePair.getFirstLsm()).getAbsolutePath()),
                                            new ServiceArg("-lsm2", SampleServicesUtils.getImageFile(args.sampleDataDir, mcd.tilePair.getSecondLsm()).getAbsolutePath()),
                                            new ServiceArg("-microscope1", mcd.tilePair.getFirstLsm().getMicroscope()),
                                            new ServiceArg("-microscope2", mcd.tilePair.getSecondLsm().getMicroscope()),
                                            new ServiceArg("-distortionCorrection", args.applyDistortionCorrection),
                                            new ServiceArg("-multiscanVersion", multiscanBlendVersion),
                                            new ServiceArg("-output", args.sampleDataDir)
                                    );
                                    mergeChannelsService = submitDependencyIfNotPresent(jacsServiceData, mergeChannelsService);
                                } else {
                                    mergedFileName = SampleServicesUtils.getImageFile(args.sampleDataDir, mcd.tilePair.getFirstLsm()).getAbsolutePath();
                                }
                                if (mcd.isNonEmptyMapping()) {
                                    // since the channels were in the right order no re-ordering of the channels is necessary
                                    JacsServiceData mapChannelsService = vaa3dChannelMapProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                                                    .waitFor(getSampleLsmsService, mergeChannelsService)
                                                    .build(),
                                            new ServiceArg("-input", mergedFileName),
                                            new ServiceArg("-output", mergedFileName),
                                            new ServiceArg("-channelMapping", mcd.mapping)
                                    );
                                    submitDependencyIfNotPresent(jacsServiceData, mapChannelsService);
                                }
                                return mcd;
                            })
                            .reduce(new ChannelMappingsConsesus(), (ac, mcd) -> {
                                ChannelMappingsConsesus cmFromMcd = new ChannelMappingsConsesus();
                                cmFromMcd.channelMapping = LSMProcessingTools.generateOutputChannelReordering(mcd.unmergedInputChannels, mcd.outputChannels);
                                cmFromMcd.outputChannelComponents = LSMProcessingTools.extractChannelComponents(mcd.outputChannels);
                                return cmFromMcd;
                            }, channelMappingConsensusCombiner);
                    return consensus;
                })
                .reduce(new ChannelMappingsConsesus(), channelMappingConsensusCombiner);
    }

    @Override
    protected ServiceComputation<JacsServiceResult<MergeSampleTilePairsIntermediateResult>> processing(JacsServiceResult<MergeSampleTilePairsIntermediateResult> depResults) {
        return computationFactory.newCompletedComputation(depResults);
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

    private MergeChannelsData mergeUsingFlylightOrderedAlgorithm(TileLsmPair tilePair, LSMImage lsm1, LSMMetadata lsm1Metadata,
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
                continue;
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
     * @return
     */
    private Collection<String> convertDyesToChannelTags(Collection<String> dyes, Map<String, String> dyeToTagMap) {
        return dyes.stream()
                .filter(dyeToTagMap::containsKey)
                .map(dyeToTagMap::get)
                .collect(Collectors.toList());
    }

    private List<String> getOutputChannelOrder(String outputChannelOrder) {
        if (StringUtils.isBlank(outputChannelOrder)) {
            return ImmutableList.of();
        } else {
            return Splitter.on(',').splitToList(outputChannelOrder);
        }
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
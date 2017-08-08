package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
import org.janelia.jacs2.asyncservice.imageservices.MIPGenerationProcessor;
import org.janelia.jacs2.asyncservice.imageservices.StitchAndBlendResult;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dStitchAndBlendProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.RegisteredJacsNotification;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Named("sampleStitcher")
public class SampleStitchProcessor extends AbstractServiceProcessor<SampleResult> {

    private static final String STITCH_DIRNAME = "stitch";
    private static final String MIPS_DIRNAME = "mips";

    static class SampleStitchArgs extends SampleServiceArgs {
        @Parameter(names = "-mergeAlgorithm", description = "Merge algorithm", required = false)
        String mergeAlgorithm;
        @Parameter(names = "-channelDyeSpec", description = "Channel dye spec", required = false)
        String channelDyeSpec;
        @Parameter(names = "-outputChannelOrder", description = "Output channel order", required = false)
        String outputChannelOrder;
        @Parameter(names = "-generateMips", description = "If specified it generates the mips", required = false)
        boolean generateMips;
    }

    private final WrappedServiceProcessor<MergeAndGroupSampleTilePairsProcessor, List<SampleAreaResult>> mergeAndGroupSampleTilePairsProcessor;
    private final WrappedServiceProcessor<Vaa3dStitchAndBlendProcessor, StitchAndBlendResult> vaa3dStitchAndBlendProcessor;
    private final WrappedServiceProcessor<MIPGenerationProcessor, List<File>> mipGenerationProcessor;
    private final TimebasedIdentifierGenerator idGenerator;

    @Inject
    SampleStitchProcessor(ServiceComputationFactory computationFactory,
                          JacsServiceDataPersistence jacsServiceDataPersistence,
                          @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                          MergeAndGroupSampleTilePairsProcessor mergeAndGroupSampleTilePairsProcessor,
                          Vaa3dStitchAndBlendProcessor vaa3dStitchAndBlendProcessor,
                          MIPGenerationProcessor mipGenerationProcessor,
                          @JacsDefault TimebasedIdentifierGenerator idGenerator,
                          Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.mergeAndGroupSampleTilePairsProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, mergeAndGroupSampleTilePairsProcessor);
        this.vaa3dStitchAndBlendProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, vaa3dStitchAndBlendProcessor);
        this.mipGenerationProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, mipGenerationProcessor);
        this.idGenerator = idGenerator;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SampleStitchProcessor.class, new SampleStitchArgs());
    }

    @Override
    public ServiceResultHandler<SampleResult> getResultHandler() {
        return new AbstractAnyServiceResultHandler<SampleResult>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public SampleResult collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<SampleResult> intermediateResult = (JacsServiceResult<SampleResult>)depResults;
                return intermediateResult.getResult();
            }

            public SampleResult getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<SampleResult>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<SampleResult>> process(JacsServiceData jacsServiceData) {
        SampleStitchArgs args = getArgs(jacsServiceData);
        return mergeAndGroupSampleTilePairsProcessor.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Merge and group tiles")
                        .registerProcessingStageNotification(
                                FlylightSampleEvents.MERGE_LSMS,
                                jacsServiceData.getProcessingStageNotification(FlylightSampleEvents.MERGE_LSMS, new RegisteredJacsNotification().withDefaultLifecycleStages())
                                        .map(n -> n.addNotificationField("sampleId", args.sampleId)
                                                        .addNotificationField("objective", args.sampleObjective)
                                                        .addNotificationField("area", args.sampleArea)
                                        )
                        )
                        .build(),
                        new ServiceArg("-sampleId", args.sampleId),
                        new ServiceArg("-objective", args.sampleObjective),
                        new ServiceArg("-area", args.sampleArea),
                        new ServiceArg("-sampleResultsId", args.sampleResultsId),
                        new ServiceArg("-sampleDataRootDir", args.sampleDataRootDir),
                        new ServiceArg("-sampleLsmsSubDir", args.sampleLsmsSubDir),
                        new ServiceArg("-sampleSummarySubDir", args.sampleSummarySubDir),
                        new ServiceArg("-sampleSitchingSubDir", args.sampleSitchingSubDir),
                        new ServiceArg("-mergeAlgorithm", args.mergeAlgorithm),
                        new ServiceArg("-channelDyeSpec", args.channelDyeSpec),
                        new ServiceArg("-outputChannelOrder", args.outputChannelOrder)
        )
        .thenCompose((JacsServiceResult<List<SampleAreaResult>> mergeResults) -> {
            List<ServiceComputation<?>> stitchComputations = ImmutableList.copyOf(stitchTiles(jacsServiceData, mergeResults, args.generateMips));
            return computationFactory.newCompletedComputation(mergeResults)
                    .thenCombineAll(stitchComputations, (mr, groupedAreas) -> (List<SampleAreaResult>) groupedAreas);
        })
        .thenSuspendUntil(this.suspendCondition(jacsServiceData))
        .thenApply((ContinuationCond.Cond<List<SampleAreaResult>> stitchedAreaResultsCond) -> {
            SampleResult sampleResult = new SampleResult();
            sampleResult.setSampleId(args.sampleId);
            sampleResult.setSampleAreaResults(stitchedAreaResultsCond.getState());
            return this.updateServiceResult(jacsServiceData, sampleResult);
        })
        ;
    }

    private List<ServiceComputation<SampleAreaResult>> stitchTiles(JacsServiceData jacsServiceData, JacsServiceResult<List<SampleAreaResult>> mergeAndGroupedResults, boolean generateMips) {
        List<SampleAreaResult> allGroupedAreasResults = mergeAndGroupedResults.getResult();
        Function<SampleAreaResult, String> stitchedFileNameGenerator;
        if (canUseAreaNameAsMergeFile(allGroupedAreasResults)) {
            stitchedFileNameGenerator = groupedArea -> {
                StringBuilder nameBuilder = new StringBuilder("stitched-");
                if (StringUtils.isNotBlank(groupedArea.getObjective()))
                    nameBuilder.append(StringUtils.replaceChars(groupedArea.getObjective(), " ", "_"));
                if (StringUtils.isNotBlank(groupedArea.getAnatomicalArea()))
                    nameBuilder.append(StringUtils.replaceChars(groupedArea.getAnatomicalArea(), " ", "_"));
                return nameBuilder.toString();
            };
        } else {
            stitchedFileNameGenerator = groupedArea -> "stitched-" + idGenerator.generateId();
        }
        return allGroupedAreasResults.stream()
                .map((SampleAreaResult groupedArea) -> {
                    Path areaResultsDir = Paths.get(groupedArea.getResultDir());
                    String referenceChannelNumber = groupedArea.getConsensusChannelComponents().referenceChannelNumbers;
                    if (groupedArea.getGroupResults().size() > 1) {
                        // if the area has more than 1 tile then stitch them together
                        Path groupDir = areaResultsDir.resolve(groupedArea.getGroupRelativeSubDir());
                        Path stitchingDir = areaResultsDir.resolve(STITCH_DIRNAME);
                        Path mipsDir = areaResultsDir.resolve(MIPS_DIRNAME);
                        Path stitchingFile = FileUtils.getFilePath(stitchingDir, stitchedFileNameGenerator.apply(groupedArea), ".v3draw");
                        return stitchTilesFromArea(jacsServiceData,
                                groupDir, stitchingFile,
                                groupedArea.getSampleId(), groupedArea.getObjective(), groupedArea.getAnatomicalArea(), referenceChannelNumber,
                                mergeAndGroupedResults.getJacsServiceData())
                                .thenCompose((JacsServiceResult<StitchAndBlendResult> stitchResult) -> {
                                    groupedArea.setStitchRelativeSubDir(STITCH_DIRNAME);
                                    groupedArea.setAreaResultFile(stitchResult.getResult().getStitchedFile().getAbsolutePath());
                                    groupedArea.setStichFile(stitchResult.getResult().getStitchedFile().getAbsolutePath());
                                    groupedArea.setStitchInfoFile(stitchResult.getResult().getStitchedImageInfoFile().getAbsolutePath());
                                    if (generateMips) {
                                        return generateMips(jacsServiceData, Paths.get(groupedArea.getStichFile()), mipsDir,
                                                groupedArea.getConsensusChannelComponents().signalChannelsPos, referenceChannelNumber,
                                                stitchResult.getJacsServiceData())
                                                .thenApply(generateMipsResult -> {
                                                    List<File> mips = generateMipsResult.getResult();
                                                    groupedArea.addMips(mips.stream().map(File::getAbsolutePath).collect(Collectors.toList()));
                                                    // if there were multiple tiles stitched together - cleanup the v3draw image files corresponding to the tiles.
                                                    if (CollectionUtils.isNotEmpty(groupedArea.getGroupResults())) {
                                                        groupedArea.getGroupResults()
                                                                .stream()
                                                                .filter((MergeTilePairResult mtpr) -> StringUtils.isNotBlank(mtpr.getMergeResultFile()))
                                                                .map((MergeTilePairResult mtpr) -> Paths.get(mtpr.getMergeResultFile()))
                                                                .forEach((Path mtprPath) -> {
                                                                    try {
                                                                        Files.deleteIfExists(mtprPath);
                                                                    } catch (IOException e) {
                                                                        logger.warn("Error while removing {}", mtprPath, e);
                                                                    }
                                                                })
                                                        ;
                                                    }
                                                    if (CollectionUtils.isNotEmpty(groupedArea.getMergeResults())) {
                                                        groupedArea.getMergeResults()
                                                                .stream()
                                                                .filter((MergeTilePairResult mtpr) -> StringUtils.isNotBlank(mtpr.getMergeResultFile()))
                                                                .map((MergeTilePairResult mtpr) -> Paths.get(mtpr.getMergeResultFile()))
                                                                .forEach((Path mtprPath) -> {
                                                                    try {
                                                                        Files.deleteIfExists(mtprPath);
                                                                    } catch (IOException e) {
                                                                        logger.warn("Error while removing {}", mtprPath, e);
                                                                    }
                                                                })
                                                        ;
                                                    }
                                                    return groupedArea;
                                                })
                                                ;
                                    } else {
                                        return computationFactory.newCompletedComputation(groupedArea);
                                    }
                                })
                                ;
                    } else if (generateMips) {
                        return groupedArea.getGroupResults().stream()
                                .filter(tp -> StringUtils.isNotBlank(tp.getMergeResultFile()))
                                .map(tp -> Paths.get(tp.getMergeResultFile()))
                                .findFirst()
                                .map(mergeResultPath -> {
                                    Path mipsDir = areaResultsDir.resolve(MIPS_DIRNAME);
                                    groupedArea.setAreaResultFile(mergeResultPath.toString());
                                    return generateMips(jacsServiceData, mergeResultPath, mipsDir,
                                            groupedArea.getConsensusChannelComponents().signalChannelsPos, referenceChannelNumber,
                                            mergeAndGroupedResults.getJacsServiceData())
                                            .thenApply(generateMipsResult -> {
                                                List<File> mips = generateMipsResult.getResult();
                                                groupedArea.addMips(mips.stream().map(File::getAbsolutePath).collect(Collectors.toList()));
                                                return groupedArea;
                                            })
                                            ;
                                })
                                .orElse(computationFactory.newCompletedComputation(groupedArea));
                    } else {
                        return computationFactory.newCompletedComputation(groupedArea);
                    }
                })
                .collect(Collectors.toList())
                ;
    }

    private boolean canUseAreaNameAsMergeFile(List<SampleAreaResult> groupedAreas) {
        long uniqueGroupedAreasCount = groupedAreas.stream()
                .map(groupedArea -> {
                    StringBuilder nameBuilder = new StringBuilder();
                    if (StringUtils.isNotBlank(groupedArea.getObjective())) nameBuilder.append(StringUtils.replaceChars(groupedArea.getObjective(), " ", "_"));
                    if (StringUtils.isNotBlank(groupedArea.getAnatomicalArea())) nameBuilder.append(StringUtils.replaceChars(groupedArea.getAnatomicalArea(), " ", "_"));
                    return nameBuilder.toString();
                })
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet())
                .size();
        return groupedAreas.size() == uniqueGroupedAreasCount;
    }

    private ServiceComputation<JacsServiceResult<StitchAndBlendResult>> stitchTilesFromArea(JacsServiceData jacsServiceData, Path inputDir, Path outputFile, Number sampleId, String objective, String area, String referenceChannelNumber, JacsServiceData... deps) {
        return vaa3dStitchAndBlendProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Stitch tiles")
                        .waitFor(deps)
                        .addRequiredMemoryInGB(72)
                        .registerProcessingNotification(
                                FlylightSampleEvents.STITCH_TILES,
                                jacsServiceData.getProcessingStageNotification(FlylightSampleEvents.STITCH_TILES, new RegisteredJacsNotification().withDefaultLifecycleStages())
                                        .map(n -> n.addNotificationField("sampleId", sampleId)
                                                        .addNotificationField("objective", objective)
                                                        .addNotificationField("area", area)
                                        )
                        )
                        .build(),
                new ServiceArg("-inputDir", inputDir.toString()),
                new ServiceArg("-outputFile", outputFile.toString()),
                new ServiceArg("-refchannel", referenceChannelNumber)
        );
    }

    private ServiceComputation<JacsServiceResult<List<File>>> generateMips(JacsServiceData jacsServiceData, Path tileFile, Path outputDir, String signalChannels, String referenceChannel, JacsServiceData... deps) {
        return mipGenerationProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Generate mips")
                        .waitFor(deps)
                        .build(),
                new ServiceArg("-inputFile", tileFile.toString()),
                new ServiceArg("-outputDir", outputDir.toString()),
                new ServiceArg("-signalChannels", signalChannels),
                new ServiceArg("-referenceChannel", referenceChannel),
                new ServiceArg("-imgFormat", "png")
        );
    }

    private SampleStitchArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SampleStitchArgs());
    }

}

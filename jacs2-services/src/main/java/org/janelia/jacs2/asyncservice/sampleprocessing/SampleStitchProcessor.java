package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.sample.AnatomicalArea;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.MIPGenerationProcessor;
import org.janelia.jacs2.asyncservice.imageservices.StitchAndBlendResult;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dStitchAndBlendProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.RegisteredJacsNotification;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Named("sampleStitcher")
public class SampleStitchProcessor extends AbstractBasicLifeCycleServiceProcessor<SampleStitchProcessor.StitchProcessingIntermediateResult, SampleResult> {

    private static final String STITCH_DIRNAME = "stitch";
    private static final String MIPS_DIRNAME = "mips";

    static class StitchProcessingIntermediateResult extends SampleIntermediateResult {
        private final List<Number> mergeTilePairServiceIds;
        private final List<AreaStitchingIntermediateResult> stitchedAreasResults = new ArrayList<>();

        StitchProcessingIntermediateResult(Number getSampleLsmsServiceDataId, List<Number> mergeTilePairServiceIds) {
            super(getSampleLsmsServiceDataId);
            this.mergeTilePairServiceIds = mergeTilePairServiceIds;
        }

        List<Number> getMergeTilePairServiceIds() {
            return mergeTilePairServiceIds;
        }
    }

    static class AreaStitchingIntermediateResult {
        private final SampleAreaResult sampleAreaResult;
        private final Optional<Path> areaResultFile;
        private Optional<Number> stichingServiceId;
        private Optional<Number> mipsServiceId;

        public AreaStitchingIntermediateResult(SampleAreaResult sampleAreaResult, Optional<Path> areaResultFile, Optional<Number> stichingServiceId, Optional<Number> mipsServiceId) {
            this.sampleAreaResult = sampleAreaResult;
            this.areaResultFile = areaResultFile;
            this.stichingServiceId = stichingServiceId;
            this.mipsServiceId = mipsServiceId;
        }
    }

    static class SampleStitchArgs extends SampleServiceArgs {
        @Parameter(names = "-mergeAlgorithm", description = "Merge algorithm", required = false)
        String mergeAlgorithm;
        @Parameter(names = "-channelDyeSpec", description = "Channel dye spec", required = false)
        String channelDyeSpec;
        @Parameter(names = "-outputChannelOrder", description = "Output channel order", required = false)
        String outputChannelOrder;
        @Parameter(names = "-distortionCorrection", description = "If specified apply distortion correction", required = false)
        boolean applyDistortionCorrection;
        @Parameter(names = "-generateMips", description = "If specified it generates the mips", required = false)
        boolean generateMips;
    }

    private final SampleDataService sampleDataService;
    private final GetSampleImageFilesProcessor getSampleImageFilesProcessor;
    private final MergeAndGroupSampleTilePairsProcessor mergeAndGroupSampleTilePairsProcessor;
    private final Vaa3dStitchAndBlendProcessor vaa3dStitchAndBlendProcessor;
    private final MIPGenerationProcessor mipGenerationProcessor;
    private final TimebasedIdentifierGenerator idGenerator;

    @Inject
    SampleStitchProcessor(ServiceComputationFactory computationFactory,
                          JacsServiceDataPersistence jacsServiceDataPersistence,
                          @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                          SampleDataService sampleDataService,
                          GetSampleImageFilesProcessor getSampleImageFilesProcessor,
                          MergeAndGroupSampleTilePairsProcessor mergeAndGroupSampleTilePairsProcessor,
                          Vaa3dStitchAndBlendProcessor vaa3dStitchAndBlendProcessor,
                          MIPGenerationProcessor mipGenerationProcessor,
                          @JacsDefault TimebasedIdentifierGenerator idGenerator,
                          Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.getSampleImageFilesProcessor = getSampleImageFilesProcessor;
        this.mergeAndGroupSampleTilePairsProcessor = mergeAndGroupSampleTilePairsProcessor;
        this.vaa3dStitchAndBlendProcessor = vaa3dStitchAndBlendProcessor;
        this.mipGenerationProcessor = mipGenerationProcessor;
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
                SampleStitchArgs args = getArgs(depResults.getJacsServiceData());
                StitchProcessingIntermediateResult result = (StitchProcessingIntermediateResult) depResults.getResult();
                SampleResult sampleResult = new SampleResult();
                sampleResult.setSampleId(args.sampleId);
                sampleResult.setSampleAreaResults(result.stitchedAreasResults.stream()
                        .map(ar -> {
                            SampleAreaResult sar = ar.sampleAreaResult;
                            if (ar.areaResultFile.isPresent()) {
                                sar.setAreaResultFile(ar.areaResultFile.get().toString());
                            }
                            return sar;
                        })
                        .collect(Collectors.toList()));
                return sampleResult;
            }

            public SampleResult getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<SampleResult>() {});
            }
        };
    }

    @Override
    protected JacsServiceResult<StitchProcessingIntermediateResult> submitServiceDependencies(JacsServiceData jacsServiceData) {
        SampleStitchArgs args = getArgs(jacsServiceData);
        // get sample's LSMs
        JacsServiceData getSampleLsmsServiceRef = getSampleImageFilesProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .build(),
                new ServiceArg("-sampleId", args.sampleId),
                new ServiceArg("-objective", args.sampleObjective),
                new ServiceArg("-area", args.sampleArea),
                new ServiceArg("-sampleDataRootDir", args.sampleDataRootDir),
                new ServiceArg("-sampleLsmsSubDir", args.sampleLsmsSubDir)
        );
        JacsServiceData getSampleLsmsService = submitDependencyIfNotFound(getSampleLsmsServiceRef);
        List<AnatomicalArea> anatomicalAreas =
                sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(jacsServiceData.getOwner(), args.sampleId, args.sampleObjective, args.sampleArea);
        // invoke child file copy services for all LSM files
        List<Number> mergeTilePairServiceIds = anatomicalAreas.stream()
                .map(ar -> {
                    // merge sample LSMs if needed
                    JacsServiceData mergeTilePairsService = mergeAndGroupSampleTilePairsProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                                    .waitFor(getSampleLsmsService)
                                    .registerProcessingStageNotification(
                                            FlylightSampleEvents.MERGE_LSMS,
                                            jacsServiceData.getProcessingStageNotification(FlylightSampleEvents.MERGE_LSMS, new RegisteredJacsNotification().withDefaultLifecycleStages())
                                                    .map(n -> n.addNotificationField("sampleId", args.sampleId)
                                                                    .addNotificationField("objective", ar.getObjective())
                                                                    .addNotificationField("area", ar.getName())
                                                    )
                                    )
                                    .build(),
                            new ServiceArg("-sampleId", args.sampleId),
                            new ServiceArg("-objective", ar.getObjective()),
                            new ServiceArg("-area", ar.getName()),
                            new ServiceArg("-sampleResultsId", args.sampleResultsId),
                            new ServiceArg("-sampleDataRootDir", args.sampleDataRootDir),
                            new ServiceArg("-sampleLsmsSubDir", args.sampleLsmsSubDir),
                            new ServiceArg("-sampleSummarySubDir", args.sampleSummarySubDir),
                            new ServiceArg("-sampleSitchingSubDir", args.sampleSitchingSubDir),
                            new ServiceArg("-mergeAlgorithm", args.mergeAlgorithm),
                            new ServiceArg("-channelDyeSpec", args.channelDyeSpec),
                            new ServiceArg("-outputChannelOrder", args.outputChannelOrder),
                            new ServiceArg("-distortionCorrection", args.applyDistortionCorrection)
                    );
                    mergeTilePairsService = submitDependencyIfNotFound(mergeTilePairsService);
                    return mergeTilePairsService;
                })
                .map(JacsServiceData::getId)
                .collect(Collectors.toList())
                ;

        return new JacsServiceResult<>(jacsServiceData, new StitchProcessingIntermediateResult(getSampleLsmsService.getId(), mergeTilePairServiceIds));
    }

    @Override
    protected ServiceComputation<JacsServiceResult<StitchProcessingIntermediateResult>> processing(JacsServiceResult<StitchProcessingIntermediateResult> depResults) {
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    pd.getResult().stitchedAreasResults.addAll(stitchTiles(pd));
                    return pd;
                })
                .thenSuspendUntil(pd -> new ContinuationCond.Cond<>(pd, !suspendUntilAllDependenciesComplete(pd.getJacsServiceData())))
                .thenApply(pdCond -> {
                    JacsServiceResult<StitchProcessingIntermediateResult> pd = pdCond.getState();
                    pd.getResult().stitchedAreasResults.stream()
                            .forEach(this::updateStitchingResult)
                    ;
                    return pd;
                })
                ;
    }

    private List<AreaStitchingIntermediateResult> stitchTiles(JacsServiceResult<StitchProcessingIntermediateResult> depResults) {
        SampleStitchArgs args = getArgs(depResults.getJacsServiceData());
        List<SampleAreaResult> allGroupedAreasResults = depResults.getResult().getMergeTilePairServiceIds().stream()
                .flatMap(mtpsId -> {
                    JacsServiceData mergeTilePairService = jacsServiceDataPersistence.findById(mtpsId);
                    List<SampleAreaResult> mergeTilePairResults = mergeAndGroupSampleTilePairsProcessor.getResultHandler().getServiceDataResult(mergeTilePairService);
                    return mergeTilePairResults.stream();
                })
                .collect(Collectors.toList());
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
                    JacsServiceData stitichingService = null;
                    Path areaResultsDir = Paths.get(groupedArea.getResultDir());
                    Optional<Path> tileFile;
                    Optional<Number> stitchingServiceId;
                    Optional<Number> mipsServiceId;
                    Path mipsDir;
                    if (groupedArea.getGroupResults().size() > 1) {
                        // if the area has more than 1 tile then stitch them together
                        Path groupDir = areaResultsDir.resolve(groupedArea.getGroupRelativeSubDir());
                        Path stitchingDir = areaResultsDir.resolve(STITCH_DIRNAME);
                        mipsDir = areaResultsDir.resolve(MIPS_DIRNAME);
                        Path stitchingFile = FileUtils.getFilePath(stitchingDir, stitchedFileNameGenerator.apply(groupedArea), ".v3draw");
                        String referenceChannelNumber = groupedArea.getConsensusChannelComponents().referenceChannelNumbers;
                        stitichingService = stitchTilesFromArea(depResults.getJacsServiceData(), groupDir, stitchingFile,
                                groupedArea.getSampleId(), groupedArea.getObjective(), groupedArea.getAnatomicalArea(), referenceChannelNumber);
                        // use the stitched file to generate the mips
                        tileFile = Optional.of(stitchingFile);
                        groupedArea.setStitchRelativeSubDir(STITCH_DIRNAME);
                        stitchingServiceId = Optional.of(stitichingService.getId());
                    } else {
                        // no need for stitching so simply take the result of the merge to generate the mips
                        tileFile = groupedArea.getGroupResults().stream()
                                .map(tp -> Paths.get(tp.getMergeResultFile()))
                                .findFirst();
                        if (tileFile.isPresent()) {
                            mipsDir = areaResultsDir.resolve(MIPS_DIRNAME);
                        } else {
                            mipsDir = null;
                        }
                        stitchingServiceId = Optional.empty();
                    }
                    if (args.generateMips && tileFile.isPresent()) {
                        JacsServiceData mipsService = generateMips(depResults.getJacsServiceData(), tileFile.get(), mipsDir,
                                groupedArea.getConsensusChannelComponents().signalChannelsPos,
                                groupedArea.getConsensusChannelComponents().referenceChannelsPos,
                                stitichingService);
                        groupedArea.setMipsRelativeSubDir(MIPS_DIRNAME);
                        mipsServiceId = Optional.of(mipsService.getId());
                    } else {
                        mipsServiceId = Optional.empty();
                    }
                    return new AreaStitchingIntermediateResult(groupedArea, tileFile, stitchingServiceId, mipsServiceId);
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

    private JacsServiceData stitchTilesFromArea(JacsServiceData jacsServiceData, Path inputDir, Path outputFile, Number sampleId, String objective, String area, String referenceChannelNumber) {
        JacsServiceData stitchingService = vaa3dStitchAndBlendProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Stitch tiles")
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
        return submitDependencyIfNotFound(stitchingService);
    }

    private JacsServiceData generateMips(JacsServiceData jacsServiceData, Path tileFile, Path outputDir, String signalChannels, String referenceChannel, JacsServiceData... deps) {
        JacsServiceData mipsService = mipGenerationProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Generate mips")
                        .waitFor(deps)
                        .build(),
                new ServiceArg("-inputFile", tileFile.toString()),
                new ServiceArg("-outputDir", outputDir.toString()),
                new ServiceArg("-signalChannels", signalChannels),
                new ServiceArg("-referenceChannel", referenceChannel),
                new ServiceArg("-imgFormat", "png")
        );
        return submitDependencyIfNotFound(mipsService);
    }

    private void updateStitchingResult(AreaStitchingIntermediateResult areaResults) {
        areaResults.stichingServiceId.ifPresent(serviceId -> {
            JacsServiceData sd = jacsServiceDataPersistence.findById(serviceId);
            StitchAndBlendResult stitchResult = vaa3dStitchAndBlendProcessor.getResultHandler().getServiceDataResult(sd);
            areaResults.sampleAreaResult.setStitchInfoFile(stitchResult.getStitchedImageInfoFile().getAbsolutePath());
            areaResults.sampleAreaResult.setStichFile(stitchResult.getStitchedFile().getAbsolutePath());
        });
        areaResults.mipsServiceId.ifPresent(serviceId -> {
            JacsServiceData sd = jacsServiceDataPersistence.findById(serviceId);
            List<File> mips = mipGenerationProcessor.getResultHandler().getServiceDataResult(sd);
            areaResults.sampleAreaResult.addMips(mips.stream().map(File::getAbsolutePath).collect(Collectors.toList()));
        });
    }

    private SampleStitchArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(jacsServiceData.getArgsArray(), new SampleStitchArgs());
    }

}

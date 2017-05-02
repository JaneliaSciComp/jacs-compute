package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.sample.AnatomicalArea;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.MIPGenerationProcessor;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dStitchAndBlendProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;
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
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Named("flylight")
public class FlylightSampleProcessor extends AbstractBasicLifeCycleServiceProcessor<FlylightSampleProcessor.FlylightProcessingIntermediateResult, Void> {

    private static final String STITCH_DIRNAME = "stitch";
    private static final String MIPS_DIRNAME = "mips";

    static class FlylightProcessingIntermediateResult extends GetSampleLsmsIntermediateResult {

        private final List<Number> mergeTilePairServiceIds;

        FlylightProcessingIntermediateResult(Number getSampleLsmsServiceDataId, List<Number> mergeTilePairServiceIds) {
            super(getSampleLsmsServiceDataId);
            this.mergeTilePairServiceIds = mergeTilePairServiceIds;
        }

    }

    static class FlylightPipelineArgs extends SampleServiceArgs {
        @Parameter(names = "-mergeAlgorithm", description = "Merge algorithm", required = false)
        String mergeAlgorithm;
        @Parameter(names = "-stitchAlgorithm", description = "Stitching algorithm", required = false)
        String stitchAlgorithm;
        @Parameter(names = "-analysisAlgorithm", description = "Analysis algorithm", required = false)
        String analysisAlgorithm;
        @Parameter(names = "-channelDyeSpec", description = "Channel dye spec", required = false)
        String channelDyeSpec;
        @Parameter(names = "-outputChannelOrder", description = "Output channel order", required = false)
        String outputChannelOrder;
        @Parameter(names = "-distortionCorrection", description = "If specified apply distortion correction", required = false)
        boolean applyDistortionCorrection;
        @Parameter(names = "persistResults", description = "If specified it generates the mips and persists the result", required = false)
        boolean persistResults;
    }

    private final SampleDataService sampleDataService;
    private final GetSampleImageFilesProcessor getSampleImageFilesProcessor;
    private final MergeAndGroupSampleTilePairsProcessor mergeAndGroupSampleTilePairsProcessor;
    private final Vaa3dStitchAndBlendProcessor vaa3dStitchAndBlendProcessor;
    private final MIPGenerationProcessor mipGenerationProcessor;
    private final TimebasedIdentifierGenerator identifierGenerator;

    @Inject
    FlylightSampleProcessor(ServiceComputationFactory computationFactory,
                            JacsServiceDataPersistence jacsServiceDataPersistence,
                            @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                            SampleDataService sampleDataService,
                            GetSampleImageFilesProcessor getSampleImageFilesProcessor,
                            MergeAndGroupSampleTilePairsProcessor mergeAndGroupSampleTilePairsProcessor,
                            Vaa3dStitchAndBlendProcessor vaa3dStitchAndBlendProcessor,
                            MIPGenerationProcessor mipGenerationProcessor,
                            TimebasedIdentifierGenerator identifierGenerator,
                            Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.getSampleImageFilesProcessor = getSampleImageFilesProcessor;
        this.mergeAndGroupSampleTilePairsProcessor = mergeAndGroupSampleTilePairsProcessor;
        this.vaa3dStitchAndBlendProcessor = vaa3dStitchAndBlendProcessor;
        this.mipGenerationProcessor = mipGenerationProcessor;
        this.identifierGenerator = identifierGenerator;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(this.getClass(), new SampleServiceArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new AbstractAnyServiceResultHandler<Void>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public Void collectResult(JacsServiceResult<?> depResults) {
                return null;
            }

            @Override
            public Void getServiceDataResult(JacsServiceData jacsServiceData) {
                return null;
            }
        };
    }

    @Override
    protected JacsServiceResult<FlylightProcessingIntermediateResult> submitServiceDependencies(JacsServiceData jacsServiceData) {
        FlylightPipelineArgs args = getArgs(jacsServiceData);
        // get sample's LSMs
        JacsServiceData getSampleLsmMetadataServiceRef = getSampleImageFilesProcessor.createServiceData(new ServiceExecutionContext(jacsServiceData),
                new ServiceArg("-sampleId", args.sampleId.toString()),
                new ServiceArg("-objective", args.sampleObjective),
                new ServiceArg("-area", args.sampleArea),
                new ServiceArg("-sampleDataDir", args.sampleDataDir)
        );
        JacsServiceData getSampleLsmMetadataService = submitDependencyIfNotPresent(jacsServiceData, getSampleLsmMetadataServiceRef);
        List<AnatomicalArea> anatomicalAreas =
                sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(jacsServiceData.getOwner(), args.sampleId, args.sampleObjective, args.sampleArea);
        // invoke child file copy services for all LSM files
        List<Number> mergeTilePairServiceIds = anatomicalAreas.stream()
                .map(ar -> {
                    // merge sample LSMs if needed
                    JacsServiceData mergeTilePairsService = mergeAndGroupSampleTilePairsProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                                    .waitFor(getSampleLsmMetadataService)
                                    .build(),
                            new ServiceArg("-sampleId", args.sampleId.toString()),
                            new ServiceArg("-area", ar.getName()),
                            new ServiceArg("-objective", ar.getObjective()),
                            new ServiceArg("-sampleDataDir", args.sampleDataDir),
                            new ServiceArg("-mergeAlgorithm", args.mergeAlgorithm),
                            new ServiceArg("-channelDyeSpec", args.channelDyeSpec),
                            new ServiceArg("-outputChannelOrder", args.outputChannelOrder),
                            new ServiceArg("-distortionCorrection", args.applyDistortionCorrection)
                    );
                    mergeTilePairsService = submitDependencyIfNotPresent(jacsServiceData, mergeTilePairsService);
                    return mergeTilePairsService;
                })
                .map(JacsServiceData::getId)
                .collect(Collectors.toList())
                ;

        return new JacsServiceResult<>(jacsServiceData, new FlylightProcessingIntermediateResult(getSampleLsmMetadataService.getId(), mergeTilePairServiceIds));
    }

    @Override
    protected ServiceComputation<JacsServiceResult<FlylightProcessingIntermediateResult>> processing(JacsServiceResult<FlylightProcessingIntermediateResult> depResults) {
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    stitchTiles(pd);
                    return pd;
                })
                ;
    }

    private void stitchTiles(JacsServiceResult<FlylightProcessingIntermediateResult> depResults) {
        FlylightPipelineArgs args = getArgs(depResults.getJacsServiceData());
        List<MergedAndGroupedAreaResult> allGroupedAreasResults = depResults.getResult().mergeTilePairServiceIds.stream()
                .flatMap(mtpsId -> {
                    JacsServiceData mergeTilePairService = jacsServiceDataPersistence.findById(mtpsId);
                    List<MergedAndGroupedAreaResult> mergeTilePairResults = mergeAndGroupSampleTilePairsProcessor.getResultHandler().getServiceDataResult(mergeTilePairService);
                    return mergeTilePairResults.stream();
                })
                .collect(Collectors.toList());
        Function<MergedAndGroupedAreaResult, String> stitchedFileNameGenerator;
        if (canUseAreaNameAsMergeFile(allGroupedAreasResults)) {
            stitchedFileNameGenerator = groupedArea -> {
                StringBuilder nameBuilder = new StringBuilder("stitched-");
                if (StringUtils.isNotBlank(groupedArea.getObjective())) nameBuilder.append(StringUtils.replaceChars(groupedArea.getObjective(), " ", "_"));
                if (StringUtils.isNotBlank(groupedArea.getAnatomicalArea())) nameBuilder.append(StringUtils.replaceChars(groupedArea.getAnatomicalArea(), " ", "_"));
                return nameBuilder.toString();
            };
        } else {
            stitchedFileNameGenerator = groupedArea -> "stitched-" + identifierGenerator.generateId();
        }
        allGroupedAreasResults.stream()
                .forEach(groupedArea -> {
                    JacsServiceData stitichingService = null;
                    Optional<Path> tileFile;
                    if (groupedArea.getGroupResults().size() > 1) {
                        Path groupDir = Paths.get(groupedArea.getGroupDir());
                        Path stitchingDir = groupDir.getParent().resolve(STITCH_DIRNAME);
                        Path stitchingFile = stitchingDir.resolve(stitchedFileNameGenerator.apply(groupedArea));
                        String referenceChannelNumber = groupedArea.getConsensusChannelComponents().referenceChannelNumbers;
                        stitichingService = stitchTilesFromArea(depResults.getJacsServiceData(), groupDir, stitchingFile, referenceChannelNumber);
                        tileFile = Optional.of(stitchingFile);
                    } else {
                        tileFile = groupedArea.getGroupResults().stream()
                                .map(tp -> Paths.get(tp.getMergeResultFile()))
                                .findFirst();
                    }
                    if (args.persistResults && tileFile.isPresent()) {
                        Path groupDir = Paths.get(groupedArea.getGroupDir());
                        Path mipsDir = groupDir.getParent().resolve(MIPS_DIRNAME);
                        generateMips(depResults.getJacsServiceData(), tileFile.get(), mipsDir,
                                groupedArea.getConsensusChannelComponents().signalChannelsPos,
                                groupedArea.getConsensusChannelComponents().referenceChannelsPos,
                                stitichingService);
                    }
                })
        ;
    }

    private boolean canUseAreaNameAsMergeFile(List<MergedAndGroupedAreaResult> groupedAreas) {
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

    private JacsServiceData stitchTilesFromArea(JacsServiceData jacsServiceData, Path inputDir, Path outputFile, String referenceChannelNumber) {
        JacsServiceData stitchingService = vaa3dStitchAndBlendProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Stitch tiles")
                        .build(),
                new ServiceArg("-inputDir", inputDir.toString()),
                new ServiceArg("-outputFile", outputFile.toString()),
                new ServiceArg("-refchannel", referenceChannelNumber)
        );
        return submitDependencyIfNotPresent(jacsServiceData, stitchingService);
    }

    private JacsServiceData generateMips(JacsServiceData jacsServiceData, Path tileFile, Path outputDir, String signalChannels, String referenceChannel, JacsServiceData... deps) {
        JacsServiceData mipsService = mipGenerationProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Generate mips")
                        .build(),
                new ServiceArg("-inputFile", tileFile.toString()),
                new ServiceArg("-outputDir", outputDir.toString()),
                new ServiceArg("-signalChannels", signalChannels),
                new ServiceArg("-referenceChannel", referenceChannel),
                new ServiceArg("-imgFormat", "png")
        );
        return submitDependencyIfNotPresent(jacsServiceData, mipsService);
    }

    private FlylightPipelineArgs getArgs(JacsServiceData jacsServiceData) {
        return SampleServiceArgs.parse(jacsServiceData.getArgsArray(), new FlylightPipelineArgs());
    }
}

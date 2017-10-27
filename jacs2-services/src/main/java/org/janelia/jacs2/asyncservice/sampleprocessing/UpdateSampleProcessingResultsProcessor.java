package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.model.jacs2.domain.enums.FileType;
import org.janelia.model.jacs2.domain.sample.FileGroup;
import org.janelia.model.jacs2.domain.sample.Sample;
import org.janelia.model.jacs2.domain.sample.SampleProcessingResult;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.stitching.StitchedImageInfo;
import org.janelia.jacs2.asyncservice.imageservices.stitching.StitchingUtils;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The services reads the results returned the specified stitch processor and adds them to the given pipeline run.
 */
@Named("updateSampleProcessingResults")
public class UpdateSampleProcessingResultsProcessor extends AbstractBasicLifeCycleServiceProcessor<List<SampleProcessorResult>, List<SampleProcessorResult>> {

    static class UpdateSampleResultsArgs extends ServiceArgs {
        @Parameter(names = "-sampleResultsId", description = "Sample run Id to receive the results", required = true)
        Long sampleResultsId;
        @Parameter(names = "-sampleProcessingId", description = "Sample processing service ID", required = true)
        Long sampleProcessingId;
    }

    private final SampleDataService sampleDataService;
    private final SampleStitchProcessor sampleStitchProcessor;
    private final TimebasedIdentifierGenerator idGenerator;

    @Inject
    UpdateSampleProcessingResultsProcessor(ServiceComputationFactory computationFactory,
                                           JacsServiceDataPersistence jacsServiceDataPersistence,
                                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                           SampleDataService sampleDataService,
                                           SampleStitchProcessor sampleStitchProcessor,
                                           @JacsDefault TimebasedIdentifierGenerator idGenerator,
                                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.sampleStitchProcessor = sampleStitchProcessor;
        this.idGenerator = idGenerator;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(UpdateSampleProcessingResultsProcessor.class, new UpdateSampleResultsArgs());
    }

    @Override
    public ServiceResultHandler<List<SampleProcessorResult>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<SampleProcessorResult>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @SuppressWarnings("unchecked")
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
    protected JacsServiceResult<List<SampleProcessorResult>> submitServiceDependencies(JacsServiceData jacsServiceData) {
        return new JacsServiceResult<>(jacsServiceData, new ArrayList<>());
    }

    @Override
    protected ServiceComputation<JacsServiceResult<List<SampleProcessorResult>>> processing(JacsServiceResult<List<SampleProcessorResult>> depResults) {
        UpdateSampleResultsArgs args = getArgs(depResults.getJacsServiceData());
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    JacsServiceData sampleProcessingService = jacsServiceDataPersistence.findById(args.sampleProcessingId);
                    SampleResult sampleResult = sampleStitchProcessor.getResultHandler().getServiceDataResult(sampleProcessingService);
                    Sample sample = sampleDataService.getSampleById(sampleProcessingService.getOwner(), sampleResult.getSampleId());

                    // for all objectives collect the results and create the corresponding samplepipeline results
                    sampleResult.getSampleAreaResults().stream()
                            .forEach((SampleAreaResult sar) -> {
                                Optional<SampleProcessingResult> optionalSampleProcessingResult = createObjectivePipelineRunResult(sample, sar);
                                if (optionalSampleProcessingResult.isPresent()) {
                                    SampleProcessingResult sampleProcessingResult = optionalSampleProcessingResult.get();

                                    SampleProcessorResult updateResult = new SampleProcessorResult();
                                    updateResult.setSampleId(sampleResult.getSampleId());
                                    updateResult.setObjective(sar.getObjective());
                                    updateResult.setArea(sar.getAnatomicalArea());
                                    updateResult.setResultDir(sar.getResultDir());
                                    updateResult.setAreaFile(sar.getAreaResultFile());
                                    updateResult.setRunId(args.sampleResultsId);
                                    updateResult.setResultId(sampleProcessingResult.getId());
                                    updateResult.setSignalChannels(sar.getConsensusChannelComponents().signalChannelsPos);
                                    updateResult.setReferenceChannel(sar.getConsensusChannelComponents().referenceChannelsPos);
                                    updateResult.setReferenceChannelNumber(sar.getConsensusChannelComponents().referenceChannelNumbers);
                                    updateResult.setChanSpec(sar.getConsensusChannelComponents().channelSpec);
                                    updateResult.setNumChannels(sar.getConsensusChannelComponents().getNumChannels());
                                    updateResult.setOpticalResolution(sampleProcessingResult.getOpticalResolution());
                                    updateResult.setImageSize(sampleProcessingResult.getImageSize());

                                    sampleDataService.addSampleObjectivePipelineRunResult(sample, sar.getObjective(), args.sampleResultsId, null, sampleProcessingResult);

                                    pd.getResult().add(updateResult);
                                }
                            });
                    return pd;
                });
    }

    @Override
    protected Function<JacsServiceData, Stream<JacsServiceData>> dependenciesGetterFunc() {
        return (JacsServiceData serviceData) -> {
            UpdateSampleResultsArgs args = getArgs(serviceData);
            return Stream.concat(
                    jacsServiceDataPersistence.findServiceDependencies(serviceData).stream(),
                    Stream.of(jacsServiceDataPersistence.findById(args.sampleProcessingId))
            );
        };
    }

    private Optional<SampleProcessingResult> createObjectivePipelineRunResult(Sample sample, SampleAreaResult areaResult) {
        return sample.lookupObjective(areaResult.getObjective())
                .map(objective -> {
                    // create entry for the corresponding service run
                    // create stitch result
                    SampleProcessingResult stitchResult = new SampleProcessingResult();
                    stitchResult.setId(idGenerator.generateId());
                    stitchResult.setName(String.format("Sample processing results (%s)", areaResult.getAnatomicalArea()));
                    stitchResult.setFilepath(areaResult.getResultDir());
                    stitchResult.setChannelSpec(areaResult.getConsensusChannelComponents().channelSpec);
                    stitchResult.setAnatomicalArea(areaResult.getAnatomicalArea());
                    List<FileGroup> fGroups = SampleServicesUtils.streamFileGroups(areaResult.getResultDir(), areaResult.getMipsFileList()).map(SampleServicesUtils::normalize).collect(Collectors.toList());
                    SampleServicesUtils.updateFiles(stitchResult, fGroups);
                    if (StringUtils.isNotBlank(areaResult.getAreaResultFile())) {
                        stitchResult.setFileName(FileType.LosslessStack, Paths.get(areaResult.getResultDir()).relativize(Paths.get(areaResult.getAreaResultFile())).toString());
                    }
                    if (StringUtils.isNotBlank(areaResult.getStitchInfoFile())) {
                        StitchedImageInfo stitchedImageInfo = StitchingUtils.readStitchedImageInfo(Paths.get(areaResult.getStitchInfoFile()));
                        stitchResult.setImageSize(stitchedImageInfo.getXYZDimensions());
                    } else {
                        stitchResult.setImageSize(getConsensusValue(areaResult.getMergeResults(), MergeTilePairResult::getImageSize));
                    }
                    stitchResult.setChannelColors(getConsensusValue(areaResult.getMergeResults(), mtp -> {
                        if (CollectionUtils.isNotEmpty(mtp.getChannelColors())) {
                            return String.join(",", mtp.getChannelColors());
                        } else {
                            return "";
                        }
                    }));
                    stitchResult.setOpticalResolution(getConsensusValue(areaResult.getMergeResults(), MergeTilePairResult::getOpticalResolution));
                    return stitchResult;
                });
    }

    private String getConsensusValue(List<MergeTilePairResult> mergeResults, Function<MergeTilePairResult, String>  tilePairResultMapper) {
        return mergeResults.stream()
                .reduce(null, (String acValue, MergeTilePairResult mtp) -> {
                    String mtpValue = tilePairResultMapper.apply(mtp);
                    if (acValue == null) {
                        return mtpValue;
                    } else if (acValue.equalsIgnoreCase(mtpValue)) {
                        return acValue;
                    } else {
                        return "";
                    }
                }, (String v1, String v2) -> {
                    if (v1 == null) return v2;
                    else if (v2 == null) return v1;
                    else if (v1.equalsIgnoreCase(v2)) {
                        return v1;
                    } else {
                        return "";
                    }
                });
    }

    private UpdateSampleResultsArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new UpdateSampleResultsArgs());
    }

}

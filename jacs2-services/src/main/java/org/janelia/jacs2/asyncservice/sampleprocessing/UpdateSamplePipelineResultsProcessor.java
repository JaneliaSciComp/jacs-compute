package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.sample.FileGroup;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.it.jacs.model.domain.sample.SamplePipelineRun;
import org.janelia.it.jacs.model.domain.sample.SampleProcessingResult;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
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
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

@Named("updateSampleResults")
public class UpdateSamplePipelineResultsProcessor extends AbstractBasicLifeCycleServiceProcessor<List<SampleProcessorResult>, List<SampleProcessorResult>> {

    static class UpdateSampleResultsArgs extends ServiceArgs {
        @Parameter(names = "-stitchingServiceId", description = "Stitching service ID", required = true)
        Long stitchingServiceId;
    }

    private final SampleDataService sampleDataService;
    private final SampleStitchProcessor sampleStitchProcessor;
    private final TimebasedIdentifierGenerator idGenerator;

    @Inject
    UpdateSamplePipelineResultsProcessor(ServiceComputationFactory computationFactory,
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
        return ServiceArgs.getMetadata(UpdateSamplePipelineResultsProcessor.class, new UpdateSampleResultsArgs());
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
                .thenSuspendUntil(pd -> new ContinuationCond.Cond<>(pd, !suspendUntilAllDependenciesComplete(pd.getJacsServiceData())))
                .thenApply(pdCond -> {
                    JacsServiceResult<List<SampleProcessorResult>> pd = pdCond.getState();
                    JacsServiceData stitchingService = jacsServiceDataPersistence.findById(args.stitchingServiceId);
                    SampleResult sampleResult = sampleStitchProcessor.getResultHandler().getServiceDataResult(stitchingService);
                    Sample sample = sampleDataService.getSampleById(stitchingService.getOwner(), sampleResult.getSampleId());

                    Map<String, SamplePipelineRun> pipelineRunsByObjective = new HashMap<>();

                    // for all objectives collect the results and create the corresponding samplepipeline results
                    sampleResult.getSampleAreaResults().stream()
                            .forEach(sar -> {
                                Optional<SampleProcessingResult> sampleProcessingResult = getObjectivePipelineRunResult(stitchingService, sample, sar);
                                if (sampleProcessingResult.isPresent()) {
                                    SamplePipelineRun pipelineRun = pipelineRunsByObjective.get(sar.getObjective());
                                    if (pipelineRun == null) {
                                        pipelineRun = new SamplePipelineRun();
                                        pipelineRun.setId(stitchingService.getId());
                                        pipelineRun.setName(StringUtils.defaultIfBlank(stitchingService.getDescription(), stitchingService.getName()));
                                        pipelineRun.setPipelineProcess(stitchingService.getName());
                                        pipelineRun.setCreationDate(stitchingService.getCreationDate());
                                        pipelineRunsByObjective.put(sar.getObjective(), pipelineRun);
                                    }
                                    pipelineRun.addResult(sampleProcessingResult.get());

                                    SampleProcessorResult updateResult = new SampleProcessorResult();
                                    updateResult.setSampleId(sampleResult.getSampleId());
                                    updateResult.setObjective(sar.getObjective());
                                    updateResult.setArea(sar.getAnatomicalArea());
                                    updateResult.setResultDir(sar.getResultDir());
                                    updateResult.setAreaFile(sar.getAreaResultFile());
                                    updateResult.setRunId(pipelineRun.getId());
                                    updateResult.setResultId(sampleProcessingResult.get().getId());
                                    updateResult.setSignalChannels(sar.getConsensusChannelComponents().signalChannelsPos);
                                    updateResult.setReferenceChannel(sar.getConsensusChannelComponents().referenceChannelsPos);
                                    pd.getResult().add(updateResult);
                                }
                            });
                    pipelineRunsByObjective.entrySet().stream()
                            .forEach(objectivePipelineRunEntry -> {
                                sampleDataService.addSampleObjectivePipelineRun(sample,
                                        objectivePipelineRunEntry.getKey(),
                                        objectivePipelineRunEntry.getValue());

                            });
                    return pd;
                });
    }

    protected Function<JacsServiceData, JacsServiceResult<Boolean>> areAllDependenciesDoneFunc() {
        return sdp -> {
            UpdateSampleResultsArgs args = getArgs(sdp);
            JacsServiceData stitchingService = jacsServiceDataPersistence.findById(args.stitchingServiceId);
            if (stitchingService.hasCompletedUnsuccessfully()) {
                jacsServiceDataPersistence.updateServiceState(
                        sdp,
                        JacsServiceState.CANCELED,
                        Optional.of(JacsServiceData.createServiceEvent(
                                JacsServiceEventTypes.CANCELED,
                                String.format("Canceled because service %d finished unsuccessfully", stitchingService.getId()))));
                logger.warn("Service {} canceled because of {}", sdp, stitchingService);
                throw new ComputationException(sdp, "Service " + sdp.getId() + " canceled");
            } else if (stitchingService.hasCompletedSuccessfully()) {
                return new JacsServiceResult<>(sdp, true);
            }
            verifyAndFailIfTimeOut(sdp);
            return new JacsServiceResult<>(sdp, false);
        };
    }

    private Optional<SampleProcessingResult> getObjectivePipelineRunResult(JacsServiceData jacsServiceData, Sample sample, SampleAreaResult areaResult) {
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
                    List<FileGroup> fGroups = SampleServicesUtils.createFileGroups(areaResult.getResultDir(), areaResult.getMipsFileList());
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
                .reduce((String) null, (String acValue, MergeTilePairResult mtp) -> {
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
        return ServiceArgs.parse(jacsServiceData.getArgsArray(), new UpdateSampleResultsArgs());
    }

}

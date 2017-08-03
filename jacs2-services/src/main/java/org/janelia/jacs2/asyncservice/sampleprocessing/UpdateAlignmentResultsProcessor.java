package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.enums.AlignmentScoreType;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.it.jacs.model.domain.sample.SampleAlignmentResult;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentProcessor;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentResultFiles;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentUtils;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.tools.LSMProcessingTools;
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
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Named("updateAlignmentResults")
public class UpdateAlignmentResultsProcessor extends AbstractBasicLifeCycleServiceProcessor<AlignmentResult, AlignmentResult> {
    private final static DecimalFormat SCORE_FORMATTER = new DecimalFormat("0.0000");

    static class UpdateAlignmentResultsArgs extends ServiceArgs {
        @Parameter(names = "-sampleId", description = "Sample ID", required = true)
        Long sampleId;
        @Parameter(names = "-objective",
                description = "Optional sample objective. If specified it retrieves all sample image files, otherwise it only retrieves the ones for the given objective",
                required = false)
        String sampleObjective;
        @Parameter(names = "-area",
                description = "Optional sample area. If specified it filters images by the specified area",
                required = false)
        String sampleArea;
        @Parameter(names = "-runId", description = "ID of the run for which to set the alignment results", required = false)
        Long runId;
        @Parameter(names = "-alignmentServiceId", description = "Alignment service ID", required = true)
        Long alignmentServiceId;
        @Parameter(names = "-alignmentResultName", description = "Alignment result name", required = false)
        String alignmentResultName;
    }

    private final SampleDataService sampleDataService;
    private final AlignmentProcessor alignmentProcessor;
    private final TimebasedIdentifierGenerator idGenerator;

    @Inject
    UpdateAlignmentResultsProcessor(ServiceComputationFactory computationFactory,
                                    JacsServiceDataPersistence jacsServiceDataPersistence,
                                    @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                    SampleDataService sampleDataService,
                                    AlignmentProcessor alignmentProcessor,
                                    @JacsDefault TimebasedIdentifierGenerator idGenerator,
                                    Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.alignmentProcessor = alignmentProcessor;
        this.idGenerator = idGenerator;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(UpdateAlignmentResultsProcessor.class, new UpdateAlignmentResultsArgs());
    }

    @Override
    public ServiceResultHandler<AlignmentResult> getResultHandler() {
        return new AbstractAnyServiceResultHandler<AlignmentResult>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @SuppressWarnings("unchecked")
            @Override
            public AlignmentResult collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<AlignmentResult> intermediateResult = (JacsServiceResult<AlignmentResult>)depResults;
                return intermediateResult.getResult();
            }

            @Override
            public AlignmentResult getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<AlignmentResult>() {});
            }
        };
    }

    @Override
    protected JacsServiceResult<AlignmentResult> submitServiceDependencies(JacsServiceData jacsServiceData) {
        return new JacsServiceResult<>(jacsServiceData);
    }

    @Override
    protected ServiceComputation<JacsServiceResult<AlignmentResult>> processing(JacsServiceResult<AlignmentResult> depResults) {
        UpdateAlignmentResultsArgs args = getArgs(depResults.getJacsServiceData());
        return computationFactory.newCompletedComputation(depResults)
                .thenApply((JacsServiceResult<AlignmentResult> pd) -> {
                    JacsServiceData alignmentService = jacsServiceDataPersistence.findById(args.alignmentServiceId);
                    AlignmentResultFiles alignmentResultFiles = alignmentProcessor.getResultHandler().getServiceDataResult(alignmentService);
                    Properties alignmentProperties = AlignmentUtils.getAlignmentProperties(alignmentResultFiles.getAlignmentPropertiesFile());

                    SampleAlignmentResult sampleAlignmentResult = new SampleAlignmentResult();
                    sampleAlignmentResult.setId(idGenerator.generateId());
                    sampleAlignmentResult.setName(String.format("%s (%s)", StringUtils.defaultIfBlank(args.alignmentResultName, alignmentResultFiles.getAlgorithm()), args.sampleArea));
                    sampleAlignmentResult.setAnatomicalArea(args.sampleArea);
                    sampleAlignmentResult.setFilepath(alignmentResultFiles.getResultDir());
                    sampleAlignmentResult.setFileName(FileType.AlignmentVerificationMovie, alignmentResultFiles.getAlignmentVerificationMovie());
                    sampleAlignmentResult.setFileName(FileType.AlignedCondolidatedLabel, alignmentProperties.getProperty("neuron.masks.filename"));
                    sampleAlignmentResult.setAlignmentSpace(alignmentProperties.getProperty("alignment.space.name"));
                    sampleAlignmentResult.setOpticalResolution(alignmentProperties.getProperty("alignment.resolution.voxels"));
                    sampleAlignmentResult.setImageSize(alignmentProperties.getProperty("alignment.image.size"));
                    sampleAlignmentResult.setBoundingBox(alignmentProperties.getProperty("alignment.bounding.box"));
                    sampleAlignmentResult.setObjective(alignmentProperties.getProperty("alignment.objective"));

                    sampleAlignmentResult.addScore(AlignmentScoreType.NormalizedCrossCorrelation, getScore(alignmentProperties, "alignment.quality.score.ncc"));
                    sampleAlignmentResult.addScore(AlignmentScoreType.ModelViolation, getScore(alignmentProperties, "alignment.quality.score.jbaqm"));
                    sampleAlignmentResult.addScore(AlignmentScoreType.OverlapCoefficient, getScore(alignmentProperties, "alignment.overlap.coefficient"));
                    sampleAlignmentResult.addScore(AlignmentScoreType.ObjectPearsonCoefficient, getScore(alignmentProperties, "alignment.object.pearson.coefficient"));
                    sampleAlignmentResult.addScore(AlignmentScoreType.OtsunaObjectPearsonCoefficient, getScore(alignmentProperties, "alignment.otsuna.object.pearson.coefficient"));
                    sampleAlignmentResult.addScores(getQiScores(alignmentProperties.getProperty("alignment.quality.score.qi")));

                    String channels = alignmentProperties.getProperty("alignment.image.channels");
                    int nChannels = -1;
                    int refChannel = -1;
                    if (StringUtils.isBlank(channels)) {
                        logger.warn("Alignment output does not contain 'alignment.image.channels' property, cannot continue processing.");
                    } else {
                        nChannels = Integer.parseInt(channels);
                    }

                    String refchan = alignmentProperties.getProperty("alignment.image.refchan");
                    if (StringUtils.isBlank(refchan)) {
                        logger.warn("Alignment output does not contain 'alignment.image.refchan' property, cannot continue processing.");
                    } else {
                        refChannel = Integer.parseInt(refchan);
                    }
                    if (nChannels != -1 && refChannel != -1) {
                        sampleAlignmentResult.setChannelSpec(LSMProcessingTools.createChanSpec(nChannels, refChannel));
                    }
                    Sample sample = sampleDataService.getSampleById(alignmentService.getOwner(), args.sampleId);
                    sampleDataService.addSampleObjectivePipelineRunResult(sample, args.sampleObjective, args.runId, null, sampleAlignmentResult);

                    AlignmentResult alignmentResult = new AlignmentResult();
                    alignmentResult.setAlignmentResultFiles(alignmentResultFiles);
                    alignmentResult.setAlignmentResultId(sampleAlignmentResult.getId());
                    alignmentResult.setSampleAlignmentResult(sampleAlignmentResult);
                    pd.setResult(alignmentResult);

                    return pd;
                });
    }

    protected Function<JacsServiceData, JacsServiceResult<Boolean>> areAllDependenciesDoneFunc() {
        return sdp -> {
            UpdateAlignmentResultsArgs args = getArgs(sdp);
            JacsServiceData alignmentService = jacsServiceDataPersistence.findById(args.alignmentServiceId);
            if (alignmentService.hasCompletedUnsuccessfully()) {
                jacsServiceDataPersistence.updateServiceState(
                        sdp,
                        JacsServiceState.CANCELED,
                        Optional.of(JacsServiceData.createServiceEvent(
                                JacsServiceEventTypes.CANCELED,
                                String.format("Canceled because service %d finished unsuccessfully", alignmentService.getId()))));
                logger.warn("Service {} canceled because of {}", sdp, alignmentService);
                throw new ComputationException(sdp, "Service " + sdp.getId() + " canceled");
            } else if (alignmentService.hasCompletedSuccessfully()) {
                return new JacsServiceResult<>(sdp, true);
            }
            verifyAndFailIfTimeOut(sdp);
            return new JacsServiceResult<>(sdp, false);
        };
    }

    private UpdateAlignmentResultsArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new UpdateAlignmentResultsArgs());
    }

    private String getScore(Properties alignmentProperties, String scoreProperty) {
        String score = alignmentProperties.getProperty(scoreProperty);
        if (StringUtils.isBlank(score)) return null;
        return SCORE_FORMATTER.format(Double.parseDouble(score));
    }

    /**
     * Process the 3 comma delimitted QI scores
     * @param qiScores comma delimitted
     * @return
     */
    private Map<AlignmentScoreType, String> getQiScores(String qiScores) {
        Map<AlignmentScoreType, String> qiScoreValuesMap = new HashMap<>();
        if (StringUtils.isBlank(qiScores)) {
            return qiScoreValuesMap;
        }
        List<Double> qiScoreValues = Splitter.on(',').omitEmptyStrings().splitToList(qiScores).stream().map(Double::valueOf).collect(Collectors.toList());
        List<Double> inconsistencyScoreValues = qiScoreValues.stream().map(v -> 1 - v).collect(Collectors.toList());

        Function<List<Double>, String> joinFormattedValues = scores -> qiScoreValues.stream().map(SCORE_FORMATTER::format).reduce("", (v1, v2) -> v1 + "," + v2);
        double[] jbaWeights = new double[] {0.288, 0.462, 0.25};
        Function<List<Double>, String> jbaWeightedAverage = scores -> IntStream.range(0, jbaWeights.length)
                .mapToObj(i -> {
                    if (i < scores.size()) {
                        return Optional.of(jbaWeights[i] * scores.get(i));
                    } else {
                        return Optional.<Double>empty();
                    }
                })
                .reduce((v1, v2) -> {
                    if (v1.isPresent() && v2.isPresent()) {
                        return Optional.of(v1.get() + v2.get());
                    } else {
                        return Optional.empty();
                    }
                })
                .map(ov -> {
                    if (ov.isPresent()) {
                        return SCORE_FORMATTER.format(ov.get());
                    } else {
                        return "";
                    }
                })
                .orElse("");

        qiScoreValuesMap.put(AlignmentScoreType.Qi, jbaWeightedAverage.apply(qiScoreValues));
        qiScoreValuesMap.put(AlignmentScoreType.QiByRegion, joinFormattedValues.apply(qiScoreValues));
        qiScoreValuesMap.put(AlignmentScoreType.Inconsistency, jbaWeightedAverage.apply(inconsistencyScoreValues));
        qiScoreValuesMap.put(AlignmentScoreType.InconsistencyByRegion, joinFormattedValues.apply(inconsistencyScoreValues));
        return qiScoreValuesMap;
    }
}

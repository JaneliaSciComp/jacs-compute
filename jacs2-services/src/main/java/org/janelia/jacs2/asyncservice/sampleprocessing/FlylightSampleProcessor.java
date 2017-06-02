package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.IndexedReference;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.IntStream;

@Named("flylightSample")
public class FlylightSampleProcessor extends AbstractBasicLifeCycleServiceProcessor<SampleIntermediateResult, List<SampleProcessorResult>> {

    private final GetSampleImageFilesProcessor getSampleImageFilesProcessor;
    private final SampleLSMSummaryProcessor sampleLSMSummaryProcessor;
    private final SampleStitchProcessor sampleStitchProcessor;
    private final UpdateSamplePipelineResultsProcessor updateSamplePipelineResultsProcessor;
    private final SampleNeuronSeparationProcessor sampleNeuronSeparationProcessor;

    @Inject
    FlylightSampleProcessor(ServiceComputationFactory computationFactory,
                            JacsServiceDataPersistence jacsServiceDataPersistence,
                            @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                            GetSampleImageFilesProcessor getSampleImageFilesProcessor,
                            SampleLSMSummaryProcessor sampleLSMSummaryProcessor,
                            SampleStitchProcessor sampleStitchProcessor,
                            UpdateSamplePipelineResultsProcessor updateSamplePipelineResultsProcessor,
                            SampleNeuronSeparationProcessor sampleNeuronSeparationProcessor,
                            Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.getSampleImageFilesProcessor = getSampleImageFilesProcessor;
        this.sampleLSMSummaryProcessor = sampleLSMSummaryProcessor;
        this.sampleStitchProcessor = sampleStitchProcessor;
        this.updateSamplePipelineResultsProcessor = updateSamplePipelineResultsProcessor;
        this.sampleNeuronSeparationProcessor = sampleNeuronSeparationProcessor;
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
                SampleIntermediateResult result = (SampleIntermediateResult) depResults.getResult();
                return updateSamplePipelineResultsProcessor.getResultHandler().getServiceDataResult(jacsServiceDataPersistence.findById(result.getChildServiceId()));
            }

            public List<SampleProcessorResult> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SampleProcessorResult>>() {});
            }
        };
    }

    @Override
    protected JacsServiceResult<SampleIntermediateResult> submitServiceDependencies(JacsServiceData jacsServiceData) {
        FlylightSampleArgs args = getArgs(jacsServiceData);
        String sampleId = args.sampleId.toString();
        Path sampleLsmsSubDir = SampleServicesUtils.getSampleDataSubDirs(SampleServicesUtils.DEFAULT_WORKING_LSMS_SUBDIR, jacsServiceData.getId().toString());
        Path sampleSummarySubDir = SampleServicesUtils.getSampleDataSubDirs("Summary", jacsServiceData.getId().toString());
        Path sampleStitchingSubDir = SampleServicesUtils.getSampleDataSubDirs("Sample", jacsServiceData.getId().toString());

        JacsServiceData getSampleLsmsService = getSampleLsms(jacsServiceData,
                args.sampleDataRootDir,
                sampleId,
                args.sampleObjective,
                args.sampleArea,
                sampleLsmsSubDir);

        if (!args.skipSummary) {
            lsmSummary(jacsServiceData,
                    args.sampleDataRootDir,
                    sampleId, args.sampleObjective, args.sampleArea, args.channelDyeSpec, args.basicMipMapsOptions, args.montageMipMaps,
                    sampleLsmsSubDir,
                    sampleSummarySubDir,
                    getSampleLsmsService);
        }

        JacsServiceData stitchService = stitch(jacsServiceData, args.sampleDataRootDir, sampleId, args.sampleObjective, args.sampleArea, args.mergeAlgorithm, args.channelDyeSpec, args.outputChannelOrder,
                args.applyDistortionCorrection, args.persistResults,
                sampleLsmsSubDir,
                sampleSummarySubDir,
                sampleStitchingSubDir,
                getSampleLsmsService);

        JacsServiceData resultsService = updateSampleResults(jacsServiceData, stitchService.getId(), stitchService);

        return new JacsServiceResult<>(jacsServiceData, new SampleIntermediateResult(resultsService.getId()));
    }

    @Override
    protected ServiceComputation<JacsServiceResult<SampleIntermediateResult>> processing(JacsServiceResult<SampleIntermediateResult> depResults) {
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    FlylightSampleArgs args = getArgs(depResults.getJacsServiceData());
                    if (args.runNeuronSeparation) {
                        runNeuronSeparation(depResults.getJacsServiceData(), args, depResults.getResult());
                    }
                    return pd;
                });
    }

    private JacsServiceData getSampleLsms(JacsServiceData jacsServiceData,
                                          String sampleDataRootDir,
                                          String sampleId, String objective, String area,
                                          Path sampleLsmsSubDir) {
        JacsServiceData getLsmsService = getSampleImageFilesProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Retrieve sample LSMs")
                        .build(),
                new ServiceArg("-sampleId", sampleId),
                new ServiceArg("-objective", objective),
                new ServiceArg("-area", area),
                new ServiceArg("-sampleDataRootDir", sampleDataRootDir),
                new ServiceArg("-sampleLsmsSubDir", sampleLsmsSubDir.toString())
        );
        return submitDependencyIfNotPresent(jacsServiceData, getLsmsService);
    }

    private JacsServiceData lsmSummary(JacsServiceData jacsServiceData, String sampleDataRootDir, String sampleId, String objective, String area, String channelDyeSpec, String basicMipMapsOptions,
                                       boolean montageMipMaps,
                                       Path sampleLsmsSubDir,
                                       Path sampleSummarySubDir,
                                       JacsServiceData... deps) {
        JacsServiceData lsmSummaryService = sampleLSMSummaryProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Create sample LSM summary")
                        .waitFor(deps)
                        .build(),
                new ServiceArg("-sampleId", sampleId),
                new ServiceArg("-objective", objective),
                new ServiceArg("-area", area),
                new ServiceArg("-sampleDataRootDir", sampleDataRootDir),
                new ServiceArg("-sampleLsmsSubDir", sampleLsmsSubDir.toString()),
                new ServiceArg("-sampleSummarySubDir", sampleSummarySubDir.toString()),
                new ServiceArg("-channelDyeSpec", channelDyeSpec),
                new ServiceArg("-basicMipMapsOptions", basicMipMapsOptions),
                new ServiceArg("-montageMipMaps", montageMipMaps)
        );
        return submitDependencyIfNotPresent(jacsServiceData, lsmSummaryService);
    }

    private JacsServiceData stitch(JacsServiceData jacsServiceData,
                                   String sampleDataRootDir,
                                   String sampleId, String objective, String area,
                                   String mergeAlgorithm, String channelDyeSpec, String outputChannelOrder,
                                   boolean useDistortionCorrection, boolean generateMips,
                                   Path sampleLsmsSubDir,
                                   Path sampleSummarySubDir,
                                   Path sampleStitchingSubDir,
                                   JacsServiceData... deps) {
        JacsServiceData stitchService = sampleStitchProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Stitch sample tiles")
                        .waitFor(deps)
                        .build(),
                new ServiceArg("-sampleId", sampleId),
                new ServiceArg("-objective", objective),
                new ServiceArg("-area", area),
                new ServiceArg("-sampleDataRootDir", sampleDataRootDir),
                new ServiceArg("-sampleLsmsSubDir", sampleLsmsSubDir.toString()),
                new ServiceArg("-sampleSummarySubDir", sampleSummarySubDir.toString()),
                new ServiceArg("-sampleSitchingSubDir", sampleStitchingSubDir.toString()),
                new ServiceArg("-mergeAlgorithm", mergeAlgorithm),
                new ServiceArg("-channelDyeSpec", channelDyeSpec),
                new ServiceArg("-outputChannelOrder", outputChannelOrder),
                new ServiceArg("-distortionCorrection", useDistortionCorrection),
                new ServiceArg("-generateMips", generateMips)
        );
        return submitDependencyIfNotPresent(jacsServiceData, stitchService);
    }

    private JacsServiceData updateSampleResults(JacsServiceData jacsServiceData, Number stitchingServiceId, JacsServiceData... deps) {
        JacsServiceData resultsService = updateSamplePipelineResultsProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Update sample results")
                        .waitFor(deps)
                        .build(),
                new ServiceArg("-stitchingServiceId", stitchingServiceId.toString())
        );
        return submitDependencyIfNotPresent(jacsServiceData, resultsService);
    }

    private void runNeuronSeparation(JacsServiceData jacsServiceData, FlylightSampleArgs args, SampleIntermediateResult intermediateResult) {
        JacsServiceData sampleResultsService = jacsServiceDataPersistence.findById(intermediateResult.getChildServiceId());
        List<SampleProcessorResult> sampleResults = updateSamplePipelineResultsProcessor.getResultHandler().getServiceDataResult(sampleResultsService);

        IntStream.range(0, sampleResults.size())
                .mapToObj(pos -> new IndexedReference<SampleProcessorResult>(sampleResults.get(pos), pos))
                .forEach(indexedSr -> {
                    Path outputDir;
                    SampleProcessorResult sr = indexedSr.getReference();
                    if (StringUtils.isNotBlank(sr.getArea())) {
                        outputDir = Paths.get(args.sampleDataRootDir)
                                .resolve(SampleServicesUtils.getSampleDataSubDirs("Separation", jacsServiceData.getId().toString()))
                                .resolve(sr.getArea());
                    } else if (sampleResults.size() > 1) {
                        outputDir = Paths.get(args.sampleDataRootDir)
                                .resolve(SampleServicesUtils.getSampleDataSubDirs("Separation", jacsServiceData.getId().toString()))
                                .resolve("area" + indexedSr.getPos() + 1);
                    } else {
                        outputDir = Paths.get(args.sampleDataRootDir)
                                .resolve(SampleServicesUtils.getSampleDataSubDirs("Separation", jacsServiceData.getId().toString()));
                    }
                    separateNeurons(jacsServiceData, sr, args.previousNeuronsResultFile, args.consolidatedNeuronsLabelFile, outputDir, sampleResultsService);
                });
    }

    private JacsServiceData separateNeurons(JacsServiceData jacsServiceData, SampleProcessorResult sampleResult,
                                            String previousNeuronsResultFile, String consolidatedNeuronLabelsFile,
                                            Path outputDir,
                                            JacsServiceData... deps) {
        JacsServiceData resultsService = sampleNeuronSeparationProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Separate sample neurons")
                        .waitFor(deps)
                        .build(),
                new ServiceArg("-sampleId", sampleResult.getSampleId().toString()),
                new ServiceArg("-objective", sampleResult.getObjective()),
                new ServiceArg("-runId", sampleResult.getRunId().toString()),
                new ServiceArg("-inputFile", sampleResult.getAreaFile()),
                new ServiceArg("-outputDir", outputDir.toString()),
                new ServiceArg("-signalChannels", sampleResult.getSignalChannels()),
                new ServiceArg("-referenceChannel", sampleResult.getReferenceChannel()),
                new ServiceArg("-previousResultFile", previousNeuronsResultFile),
                new ServiceArg("-consolidatedLabelFile", consolidatedNeuronLabelsFile)
        );
        return submitDependencyIfNotPresent(jacsServiceData, resultsService);
    }

    private FlylightSampleArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(jacsServiceData.getArgsArray(), new FlylightSampleArgs());
    }

}

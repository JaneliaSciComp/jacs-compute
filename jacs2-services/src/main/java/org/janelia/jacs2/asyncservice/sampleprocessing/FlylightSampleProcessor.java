package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
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
import org.janelia.jacs2.asyncservice.utils.FileUtils;
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

@Named("flylightSample")
public class FlylightSampleProcessor extends AbstractBasicLifeCycleServiceProcessor<SampleIntermediateResult, List<SampleProcessorResult>> {

    private final GetSampleImageFilesProcessor getSampleImageFilesProcessor;
    private final SampleLSMSummaryProcessor sampleLSMSummaryProcessor;
    private final SampleStitchProcessor sampleStitchProcessor;
    private final UpdateSamplePipelineResultsProcessor updateSamplePipelineResultsProcessor;

    @Inject
    FlylightSampleProcessor(ServiceComputationFactory computationFactory,
                            JacsServiceDataPersistence jacsServiceDataPersistence,
                            @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                            GetSampleImageFilesProcessor getSampleImageFilesProcessor,
                            SampleLSMSummaryProcessor sampleLSMSummaryProcessor,
                            SampleStitchProcessor sampleStitchProcessor,
                            UpdateSamplePipelineResultsProcessor updateSamplePipelineResultsProcessor,
                            Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.getSampleImageFilesProcessor = getSampleImageFilesProcessor;
        this.sampleLSMSummaryProcessor = sampleLSMSummaryProcessor;
        this.sampleStitchProcessor = sampleStitchProcessor;
        this.updateSamplePipelineResultsProcessor = updateSamplePipelineResultsProcessor;
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
        Path sampleDataDir = getSampleDataDir(jacsServiceData, args);
        String sampleId = args.sampleId.toString();

        JacsServiceData getSampleLsmsService = getSampleLsms(jacsServiceData, sampleId, args.sampleObjective, args.sampleArea, sampleDataDir);

        lsmSummary(jacsServiceData, sampleId, args.sampleObjective, args.sampleArea, args.channelDyeSpec, args.basicMipMapsOptions, args.montageMipMaps, sampleDataDir, getSampleLsmsService);

        JacsServiceData stitchService = stitch(jacsServiceData, sampleId, args.sampleObjective, args.sampleArea, args.mergeAlgorithm, args.channelDyeSpec, args.outputChannelOrder,
                args.applyDistortionCorrection, args.persistResults,
                sampleDataDir,
                getSampleLsmsService);

        JacsServiceData resultsService = updateSampleResults(jacsServiceData, stitchService.getId(), stitchService);

        return new JacsServiceResult<>(jacsServiceData, new SampleIntermediateResult(resultsService.getId()));
    }

    @Override
    protected ServiceComputation<JacsServiceResult<SampleIntermediateResult>> processing(JacsServiceResult<SampleIntermediateResult> depResults) {
        return computationFactory.newCompletedComputation(depResults);
    }

    private JacsServiceData getSampleLsms(JacsServiceData jacsServiceData, String sampleId, String objective, String area, Path sampleDataDir) {
        JacsServiceData getLsmsService = getSampleImageFilesProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Retrieve sample LSMs")
                        .build(),
                new ServiceArg("-sampleId", sampleId),
                new ServiceArg("-objective", objective),
                new ServiceArg("-area", area),
                new ServiceArg("-sampleDataDir", sampleDataDir.toString())
        );
        return submitDependencyIfNotPresent(jacsServiceData, getLsmsService);
    }

    private JacsServiceData lsmSummary(JacsServiceData jacsServiceData, String sampleId, String objective, String area, String channelDyeSpec, String basicMipMapsOptions,
                                       boolean montageMipMaps,
                                       Path sampleDataDir, JacsServiceData... deps) {
        JacsServiceData lsmSummaryService = sampleLSMSummaryProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Create sample LSM summary")
                        .waitFor(deps)
                        .build(),
                new ServiceArg("-sampleId", sampleId),
                new ServiceArg("-objective", objective),
                new ServiceArg("-area", area),
                new ServiceArg("-sampleDataDir", sampleDataDir.toString()),
                new ServiceArg("-channelDyeSpec", channelDyeSpec),
                new ServiceArg("-basicMipMapsOptions", basicMipMapsOptions),
                new ServiceArg("-montageMipMaps", montageMipMaps)
        );
        return submitDependencyIfNotPresent(jacsServiceData, lsmSummaryService);
    }

    private JacsServiceData stitch(JacsServiceData jacsServiceData, String sampleId, String objective, String area,
                                   String mergeAlgorithm, String channelDyeSpec, String outputChannelOrder,
                                   boolean useDistortionCorrection, boolean generateMips,
                                   Path sampleDataDir,
                                   JacsServiceData... deps) {
        JacsServiceData stitchService = sampleStitchProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Stitch sample tiles")
                        .waitFor(deps)
                        .build(),
                new ServiceArg("-sampleId", sampleId),
                new ServiceArg("-objective", objective),
                new ServiceArg("-area", area),
                new ServiceArg("-sampleDataDir", sampleDataDir.toString()),
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

    private FlylightSampleArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(jacsServiceData.getArgsArray(), new FlylightSampleArgs());
    }

    private Path getSampleDataDir(JacsServiceData jacsServiceData, FlylightSampleArgs args) {
        List<String> serviceIdTreePath = FileUtils.getTreePathComponentsForId(jacsServiceData.getId());
        return Paths.get(args.sampleDataDir, serviceIdTreePath.toArray(new String[serviceIdTreePath.size()]));
    }

}

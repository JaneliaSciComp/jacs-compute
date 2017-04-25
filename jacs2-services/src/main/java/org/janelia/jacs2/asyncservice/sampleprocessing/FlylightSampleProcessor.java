package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
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
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;

@Named("flylight")
public class FlylightSampleProcessor extends AbstractBasicLifeCycleServiceProcessor<FlylightSampleProcessor.FlylightProcessingIntermediateResult, Void> {

    static class FlylightProcessingIntermediateResult extends GetSampleLsmsIntermediateResult {

        FlylightProcessingIntermediateResult(Number getSampleLsmsServiceDataId) {
            super(getSampleLsmsServiceDataId);
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
    }

    private final SampleDataService sampleDataService;
    private final GetSampleImageFilesProcessor getSampleImageFilesProcessor;
    private final MergeSampleTilePairsProcessor mergeSampleTilePairsProcessor;

    @Inject
    FlylightSampleProcessor(ServiceComputationFactory computationFactory,
                            JacsServiceDataPersistence jacsServiceDataPersistence,
                            @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                            SampleDataService sampleDataService,
                            GetSampleImageFilesProcessor getSampleImageFilesProcessor,
                            MergeSampleTilePairsProcessor mergeSampleTilePairsProcessor,
                            Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.getSampleImageFilesProcessor = getSampleImageFilesProcessor;
        this.mergeSampleTilePairsProcessor = mergeSampleTilePairsProcessor;
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
        anatomicalAreas.stream()
                .map(ar -> {
                    // merge sample LSMs if needed
                    JacsServiceData mergeTilePairsService = mergeSampleTilePairsProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
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
                    return submitDependencyIfNotPresent(jacsServiceData, mergeTilePairsService);
                })
                .forEach(sd -> {});
                ;

        return new JacsServiceResult<>(jacsServiceData, new FlylightProcessingIntermediateResult(getSampleLsmMetadataService.getId()));
    }

    @Override
    protected ServiceComputation<JacsServiceResult<FlylightProcessingIntermediateResult>> processing(JacsServiceResult<FlylightProcessingIntermediateResult> depResults) {
        return computationFactory.newCompletedComputation(depResults)
                ;
    }

    private FlylightPipelineArgs getArgs(JacsServiceData jacsServiceData) {
        return SampleServiceArgs.parse(jacsServiceData.getArgsArray(), new FlylightPipelineArgs());
    }

}

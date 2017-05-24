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
public class FlylightSampleProcessor extends AbstractBasicLifeCycleServiceProcessor<FlylightSampleProcessor.FlylightSampleIntermediateResult, List<SampleAreaResult>> {

    static class FlylightSampleIntermediateResult {
        private final Number sampleStitchServiceId;

        FlylightSampleIntermediateResult(Number sampleStitchServiceId) {
            this.sampleStitchServiceId = sampleStitchServiceId;
        }

        Number getSampleStitchServiceId() {
            return sampleStitchServiceId;
        }
    }

    static class FlylightSampleArgs extends SampleServiceArgs {
        @Parameter(names = "-mergeAlgorithm", description = "Merge algorithm", required = false)
        String mergeAlgorithm;
        @Parameter(names = "-channelDyeSpec", description = "Channel dye spec", required = false)
        String channelDyeSpec;
        @Parameter(names = "-outputChannelOrder", description = "Output channel order", required = false)
        String outputChannelOrder;
        @Parameter(names = "-distortionCorrection", description = "If specified apply distortion correction", required = false)
        boolean applyDistortionCorrection;
        @Parameter(names = "-basicMipMapsOptions", description = "Basic MIPS and Movies Options", required = false)
        String basicMipMapsOptions = "mips:movies:legends:bcomp";
        @Parameter(names = "-persistResults", description = "If specified it generates the mips", required = false)
        boolean persistResults;
    }

    private final GetSampleImageFilesProcessor getSampleImageFilesProcessor;
    private final SampleLSMSummaryProcessor sampleLSMSummaryProcessor;
    private final SampleStitchProcessor sampleStitchProcessor;

    @Inject
    FlylightSampleProcessor(ServiceComputationFactory computationFactory,
                            JacsServiceDataPersistence jacsServiceDataPersistence,
                            @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                            GetSampleImageFilesProcessor getSampleImageFilesProcessor,
                            SampleLSMSummaryProcessor sampleLSMSummaryProcessor,
                            SampleStitchProcessor sampleStitchProcessor,
                            Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.getSampleImageFilesProcessor = getSampleImageFilesProcessor;
        this.sampleLSMSummaryProcessor = sampleLSMSummaryProcessor;
        this.sampleStitchProcessor = sampleStitchProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(this.getClass(), new FlylightSampleArgs());
    }

    @Override
    public ServiceResultHandler<List<SampleAreaResult>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<SampleAreaResult>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public List<SampleAreaResult> collectResult(JacsServiceResult<?> depResults) {
                FlylightSampleIntermediateResult result = (FlylightSampleIntermediateResult) depResults.getResult();
                return sampleStitchProcessor.getResultHandler().getServiceDataResult(jacsServiceDataPersistence.findById(result.getSampleStitchServiceId()));
            }

            public List<SampleAreaResult> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SampleAreaResult>>() {});
            }
        };
    }

    @Override
    protected JacsServiceResult<FlylightSampleIntermediateResult> submitServiceDependencies(JacsServiceData jacsServiceData) {
        FlylightSampleArgs args = getArgs(jacsServiceData);
        Path sampleDataDir = getSampleDataDir(jacsServiceData, args);
        String sampleId = args.sampleId.toString();

        JacsServiceData getSampleLsmsService = getSampleLsms(jacsServiceData, sampleId, args.sampleObjective, args.sampleArea, sampleDataDir);

        lsmSummary(jacsServiceData, sampleId, args.sampleObjective, args.sampleArea, args.channelDyeSpec, args.basicMipMapsOptions, sampleDataDir, getSampleLsmsService);

        JacsServiceData stitchService = stitch(jacsServiceData, sampleId, args.sampleObjective, args.sampleArea, args.mergeAlgorithm, args.channelDyeSpec, args.outputChannelOrder,
                args.applyDistortionCorrection, args.persistResults,
                sampleDataDir,
                getSampleLsmsService);

        return new JacsServiceResult<>(jacsServiceData, new FlylightSampleIntermediateResult(stitchService.getId()));
    }

    @Override
    protected ServiceComputation<JacsServiceResult<FlylightSampleIntermediateResult>> processing(JacsServiceResult<FlylightSampleIntermediateResult> depResults) {
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
                new ServiceArg("-basicMipMapsOptions", basicMipMapsOptions)
        );
        return submitDependencyIfNotPresent(jacsServiceData, lsmSummaryService);
    }

    private JacsServiceData stitch(JacsServiceData jacsServiceData, String sampleId, String objective, String area,
                                   String mergeAlgorithm, String channelDyeSpec, String outputChannelOrder,
                                   boolean useDistortionCorrection, boolean generateMips,
                                   Path sampleDataDir,
                                   JacsServiceData... deps) {
        JacsServiceData mipsService = sampleStitchProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
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
        return submitDependencyIfNotPresent(jacsServiceData, mipsService);
    }

    private FlylightSampleArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(jacsServiceData.getArgsArray(), new FlylightSampleArgs());
    }

    private Path getSampleDataDir(JacsServiceData jacsServiceData, FlylightSampleArgs args) {
        List<String> serviceIdTreePath = FileUtils.getTreePathComponentsForId(jacsServiceData.getId());
        return Paths.get(args.sampleDataDir, serviceIdTreePath.toArray(new String[serviceIdTreePath.size()]));
    }

}

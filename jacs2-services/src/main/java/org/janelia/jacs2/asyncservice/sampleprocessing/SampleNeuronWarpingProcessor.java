package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationFiles;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronWarpingProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;

@Dependent
@Named("sampleNeuronWarping")
public class SampleNeuronWarpingProcessor extends AbstractServiceProcessor<NeuronSeparationFiles> {

    static class SampleNeuronWarpingArgs extends ServiceArgs {
        @Parameter(names = "-sampleId", description = "Sample ID", required = true)
        Long sampleId;
        @Parameter(names = "-objective", description = "Sample objective for which to update the separation result.", required = false)
        String sampleObjective;
        @Parameter(names = "-runId", description = "Run ID to be updated with the corresponding fragment results.", required = true)
        Long pipelineRunId;
        @Parameter(names = "-resultId", description = "Run ID to be updated with the corresponding fragment results.", required = true)
        Long pipelineResultId;
        @Parameter(names = {"-inputFile"}, description = "Input file name", required = true)
        String inputFile;
        @Parameter(names = {"-outputDir"}, description = "Output directory name", required = true)
        String outputDir;
        @Parameter(names = "-signalChannels", description = "Signal channels")
        String signalChannels = "0 1 2";
        @Parameter(names = "-referenceChannel", description = "Reference channel")
        String referenceChannel = "3";
        @Parameter(names = "-previousResultFile", description = "Previous result file name")
        String previousResultFile;
        @Parameter(names = "-consolidatedLabelFile", description = "Consolidated label file name", required = true)
        String consolidatedLabelFile;
        @Parameter(names = "-numThreads", description = "Number of threads")
        int numThreads = 16;
    }

    private final WrappedServiceProcessor<NeuronWarpingProcessor, NeuronSeparationFiles> neuronWarpingProcessor;
    private final SampleNeuronSeparationResultHandler sampleNeuronSeparationResultHandler;

    @Inject
    SampleNeuronWarpingProcessor(ServiceComputationFactory computationFactory,
                                 JacsServiceDataPersistence jacsServiceDataPersistence,
                                 @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                 SampleDataService sampleDataService,
                                 NeuronWarpingProcessor neuronWarpingProcessor,
                                 Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.neuronWarpingProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, neuronWarpingProcessor);
        sampleNeuronSeparationResultHandler = new SampleNeuronSeparationResultHandler(sampleDataService, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SampleNeuronWarpingProcessor.class, new SampleNeuronWarpingArgs());
    }

    @Override
    public ServiceResultHandler<NeuronSeparationFiles> getResultHandler() {
        return neuronWarpingProcessor.getResultHandler();
    }

    @Override
    public ServiceComputation<JacsServiceResult<NeuronSeparationFiles>> process(JacsServiceData jacsServiceData) {
        SampleNeuronWarpingArgs args = getArgs(jacsServiceData);
        return neuronWarpingProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Warp sample neurons")
                        .registerProcessingNotification(
                                FlylightSampleEvents.NEURON_WARPING,
                                jacsServiceData.getProcessingStageNotification(FlylightSampleEvents.NEURON_WARPING, null)
                        )
                        .build(),
                new ServiceArg("-inputFile", args.inputFile),
                new ServiceArg("-outputDir", args.outputDir),
                new ServiceArg("-previousResultFile", args.previousResultFile),
                new ServiceArg("-signalChannels", args.signalChannels),
                new ServiceArg("-referenceChannel", args.referenceChannel),
                new ServiceArg("-consolidatedLabelFile", args.consolidatedLabelFile),
                new ServiceArg("-numThreads", String.valueOf(args.numThreads))
        )
        .thenApply((JacsServiceResult<NeuronSeparationFiles> jacsSeparationResult) -> {
            JacsServiceData separationServiceData = jacsSeparationResult.getJacsServiceData();
            NeuronSeparationFiles neuronSeparationFiles = jacsSeparationResult.getResult();
            Number separationServiceId = jacsSeparationResult.getJacsServiceData().getId();

            sampleNeuronSeparationResultHandler.updateSampleNeuronSeparationResult(args.sampleId, separationServiceData.getOwnerKey(),
                    args.sampleObjective, args.pipelineRunId, args.pipelineResultId, separationServiceId, separationServiceData.getDescription(), neuronSeparationFiles);

            return updateServiceResult(jacsServiceData, neuronSeparationFiles);
        });
    }

    private SampleNeuronWarpingArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SampleNeuronWarpingArgs());
    }

}

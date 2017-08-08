package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationProcessor;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationFiles;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;

@Named("sampleNeuronSeparation")
public class SampleNeuronSeparationProcessor extends AbstractServiceProcessor<NeuronSeparationFiles> {

    static class SampleNeuronSeparationArgs extends ServiceArgs {
        @Parameter(names = "-sampleId", description = "Sample ID", required = true)
        Long sampleId;
        @Parameter(names = "-objective", description = "Sample objective for which to update the separation result.", required = true)
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
        @Parameter(names = "-numThreads", description = "Number of threads")
        int numThreads = 16;
    }

    private final WrappedServiceProcessor<NeuronSeparationProcessor, NeuronSeparationFiles> neuronSeparationProcessor;
    private final SampleNeuronSeparationResultHandler sampleNeuronSeparationResultHandler;

    @Inject
    SampleNeuronSeparationProcessor(ServiceComputationFactory computationFactory,
                                    JacsServiceDataPersistence jacsServiceDataPersistence,
                                    @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                    SampleDataService sampleDataService,
                                    NeuronSeparationProcessor neuronSeparationProcessor,
                                    Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.neuronSeparationProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, neuronSeparationProcessor);
        sampleNeuronSeparationResultHandler = new SampleNeuronSeparationResultHandler(sampleDataService, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SampleNeuronSeparationProcessor.class, new SampleNeuronSeparationArgs());
    }

    @Override
    public ServiceResultHandler<NeuronSeparationFiles> getResultHandler() {
        return neuronSeparationProcessor.getResultHandler();
    }

    @Override
    public ServiceComputation<JacsServiceResult<NeuronSeparationFiles>> process(JacsServiceData jacsServiceData) {
        SampleNeuronSeparationArgs args = getArgs(jacsServiceData);
        return neuronSeparationProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Separate sample neurons")
                        .registerProcessingNotification(
                                FlylightSampleEvents.NEURON_SEPARATION,
                                jacsServiceData.getProcessingStageNotification(FlylightSampleEvents.NEURON_SEPARATION, null)
                        )
                        .build(),
                new ServiceArg("-inputFile", args.inputFile),
                new ServiceArg("-outputDir", args.outputDir),
                new ServiceArg("-previousResultFile", args.previousResultFile),
                new ServiceArg("-signalChannels", args.signalChannels),
                new ServiceArg("-referenceChannel", args.referenceChannel),
                new ServiceArg("-numThreads", String.valueOf(args.numThreads))
        )
        .thenApply((JacsServiceResult<NeuronSeparationFiles> jacsSeparationResult) -> {
            JacsServiceData separationServiceData = jacsSeparationResult.getJacsServiceData();
            NeuronSeparationFiles neuronSeparationFiles = jacsSeparationResult.getResult();

            Number separationServiceId = jacsSeparationResult.getJacsServiceData().getId();

            sampleNeuronSeparationResultHandler.updateSampleNeuronSeparationResult(args.sampleId, separationServiceData.getOwner(),
                    args.sampleObjective, args.pipelineRunId, args.pipelineResultId, separationServiceId, separationServiceData.getDescription(), neuronSeparationFiles);

            return updateServiceResult(jacsServiceData, neuronSeparationFiles);
        });
    }

    private SampleNeuronSeparationArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SampleNeuronSeparationArgs());
    }

}

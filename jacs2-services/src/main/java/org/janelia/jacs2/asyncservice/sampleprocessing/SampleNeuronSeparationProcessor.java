package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationProcessor;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronWarpingProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;

@Named("sampleNeuronSeparation")
public class SampleNeuronSeparationProcessor extends AbstractBasicLifeCycleServiceProcessor<SampleNeuronSeparationProcessor.SampleSeparationIntermediateResult, Void> {

    static class SampleSeparationIntermediateResult {
        private final Number neuronSeparationServiceId;

        public SampleSeparationIntermediateResult(Number neuronSeparationServiceId) {
            this.neuronSeparationServiceId = neuronSeparationServiceId;
        }
    }

    static class SampleNeuronSeparationArgs extends ServiceArgs {
        @Parameter(names = {"-inputFile"}, description = "Input file name", required = true)
        String inputFile;
        @Parameter(names = {"-outputDir"}, description = "Output directory name", required = true)
        String outputDir;
        @Parameter(names = "-previousResultFile", description = "Previous result file name")
        String previousResultFile;
        @Parameter(names = "-signalChannels", description = "Signal channels")
        String signalChannels = "0 1 2";
        @Parameter(names = "-referenceChannel", description = "Reference channel")
        String referenceChannel = "3";
        @Parameter(names = "-consolidatedLabelFile", description = "Consolidated label file name", required = false)
        String consolidatedLabelFile;
        @Parameter(names = "-numThreads", description = "Number of threads")
        int numThreads = 16;
    }

    private final SampleDataService sampleDataService;
    private final NeuronSeparationProcessor neuronSeparationProcessor;
    private final NeuronWarpingProcessor neuronWarpingProcessor;

    @Inject
    SampleNeuronSeparationProcessor(ServiceComputationFactory computationFactory,
                                    JacsServiceDataPersistence jacsServiceDataPersistence,
                                    @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                    SampleDataService sampleDataService,
                                    NeuronSeparationProcessor neuronSeparationProcessor,
                                    NeuronWarpingProcessor neuronWarpingProcessor,
                                    Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.neuronSeparationProcessor = neuronSeparationProcessor;
        this.neuronWarpingProcessor = neuronWarpingProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SampleNeuronSeparationProcessor.class, new SampleServiceArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @Override
    protected JacsServiceResult<SampleSeparationIntermediateResult> submitServiceDependencies(JacsServiceData jacsServiceData) {
        SampleNeuronSeparationArgs args = getArgs(jacsServiceData);
        JacsServiceData neuronSeparationService;
        if (StringUtils.isNotEmpty(args.consolidatedLabelFile)) {
            neuronSeparationService = neuronWarpingProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                            .description("Warp sample neurons")
                            .build(),
                    new ServiceArg("-inputFile", args.inputFile),
                    new ServiceArg("-outputDir", args.outputDir),
                    new ServiceArg("-previousResultFile", args.previousResultFile),
                    new ServiceArg("-signalChannels", args.signalChannels),
                    new ServiceArg("-referenceChannel", args.referenceChannel),
                    new ServiceArg("-consolidatedLabelFile", args.consolidatedLabelFile),
                    new ServiceArg("-numThreads", String.valueOf(args.numThreads))
            );
        } else {
            neuronSeparationService = neuronSeparationProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                            .description("Separate sample neurons")
                            .build(),
                    new ServiceArg("-inputFile", args.inputFile),
                    new ServiceArg("-outputDir", args.outputDir),
                    new ServiceArg("-previousResultFile", args.previousResultFile),
                    new ServiceArg("-signalChannels", args.signalChannels),
                    new ServiceArg("-referenceChannel", args.referenceChannel),
                    new ServiceArg("-numThreads", String.valueOf(args.numThreads))
            );
        }
        neuronSeparationService = submitDependencyIfNotPresent(jacsServiceData, neuronSeparationService);
        return new JacsServiceResult<>(jacsServiceData, new SampleSeparationIntermediateResult(neuronSeparationService.getId()));
    }

    @Override
    protected ServiceComputation<JacsServiceResult<SampleSeparationIntermediateResult>> processing(JacsServiceResult<SampleSeparationIntermediateResult> depResults) {
        return computationFactory.newCompletedComputation(depResults);
    }

    private SampleNeuronSeparationArgs getArgs(JacsServiceData jacsServiceData) {
        return SampleServiceArgs.parse(jacsServiceData.getArgsArray(), new SampleNeuronSeparationArgs());
    }

}

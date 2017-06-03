package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.Reference;
import org.janelia.it.jacs.model.domain.ReverseReference;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.sample.NeuronFragment;
import org.janelia.it.jacs.model.domain.sample.NeuronSeparation;
import org.janelia.it.jacs.model.domain.sample.Sample;
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
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationResult;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronWarpingProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Named("sampleNeuronSeparation")
public class SampleNeuronSeparationProcessor extends AbstractBasicLifeCycleServiceProcessor<SampleNeuronSeparationProcessor.SampleSeparationIntermediateResult, Void> {

    static class SampleSeparationIntermediateResult {
        private final Number neuronSeparationServiceId;

        public SampleSeparationIntermediateResult(Number neuronSeparationServiceId) {
            this.neuronSeparationServiceId = neuronSeparationServiceId;
        }
    }

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

    private final SampleDataService sampleDataService;
    private final NeuronSeparationProcessor neuronSeparationProcessor;

    @Inject
    SampleNeuronSeparationProcessor(ServiceComputationFactory computationFactory,
                                    JacsServiceDataPersistence jacsServiceDataPersistence,
                                    @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                    SampleDataService sampleDataService,
                                    NeuronSeparationProcessor neuronSeparationProcessor,
                                    Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.neuronSeparationProcessor = neuronSeparationProcessor;
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
        JacsServiceData neuronSeparationService = neuronSeparationProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Separate sample neurons")
                        .build(),
                new ServiceArg("-inputFile", args.inputFile),
                new ServiceArg("-outputDir", args.outputDir),
                new ServiceArg("-previousResultFile", args.previousResultFile),
                new ServiceArg("-signalChannels", args.signalChannels),
                new ServiceArg("-referenceChannel", args.referenceChannel),
                new ServiceArg("-numThreads", String.valueOf(args.numThreads))
        );
        neuronSeparationService = submitDependencyIfNotPresent(jacsServiceData, neuronSeparationService);
        return new JacsServiceResult<>(jacsServiceData, new SampleSeparationIntermediateResult(neuronSeparationService.getId()));
    }

    @Override
    protected ServiceComputation<JacsServiceResult<SampleSeparationIntermediateResult>> processing(JacsServiceResult<SampleSeparationIntermediateResult> depResults) {
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    Number separationServiceId = depResults.getResult().neuronSeparationServiceId;
                    JacsServiceData separationServiceData = jacsServiceDataPersistence.findById(separationServiceId);
                    SampleNeuronSeparationArgs args = getArgs(pd.getJacsServiceData());
                    NeuronSeparationResult separationResult = neuronSeparationProcessor.getResultHandler().getServiceDataResult(separationServiceData);
                    Sample sample = sampleDataService.getSampleById(separationServiceData.getOwner(), args.sampleId);
                    Preconditions.checkArgument(sample != null, "Invalid sample ID");
                    NeuronSeparation neuronSeparation = new NeuronSeparation();
                    neuronSeparation.setId(separationServiceId);
                    neuronSeparation.setName(separationServiceData.getName());
                    neuronSeparation.setFilepath(separationResult.getResultDir());
                    separationResult.findSeparationResult()
                            .ifPresent(srf -> neuronSeparation.setFileName(FileType.NeuronSeparatorResult, srf));
                    ReverseReference fragmentsReference = new ReverseReference();
                    fragmentsReference.setReferringClassName(NeuronFragment.class.getSimpleName());
                    fragmentsReference.setReferenceAttr("separationId");
                    fragmentsReference.setReferenceId(neuronSeparation.getId());
                    fragmentsReference.setCount((long) separationResult.getNeurons().size());
                    neuronSeparation.setFragmentsReference(fragmentsReference);

                    List<NeuronFragment> neuronFragmentList = new ArrayList<>();
                    for (String neuron : Ordering.natural().sortedCopy(separationResult.getNeurons())) {
                        Integer neuronIndex = getNeuronIndex(neuron);
                        NeuronFragment neuronFragment = new NeuronFragment();
                        neuronFragment.setOwnerKey(sample.getOwnerKey());
                        neuronFragment.setReaders(sample.getReaders());
                        neuronFragment.setWriters(sample.getWriters());
                        neuronFragment.setName("Neuron Fragment " + neuronIndex);
                        neuronFragment.setNumber(neuronIndex);
                        neuronFragment.setSample(Reference.createFor(sample));
                        neuronFragment.setSeparationId(separationServiceId);
                        neuronFragment.setFilepath(args.outputDir);
                        neuronFragment.setFileName(FileType.SignalMip, neuron);
                        Optional<String> neuronMask = separationResult.getNeuronMask(neuronIndex);
                        if (neuronMask.isPresent()) {
                            neuronFragment.setFileName(FileType.MaskFile, neuronMask.get());
                        }
                        Optional<String> neuronChan = separationResult.getNeuronChan(neuronIndex);
                        if (neuronChan.isPresent()) {
                            neuronFragment.setFileName(FileType.ChanFile, neuronChan.get());
                        }
                        neuronFragmentList.add(neuronFragment);
                    }
                    sampleDataService.addSampleObjectivePipelineRunResult(sample, args.sampleObjective, args.pipelineRunId, args.pipelineResultId, neuronSeparation);
                    sampleDataService.createNeuronFragments(neuronFragmentList);
                    return pd;
                });
    }

    private Integer getNeuronIndex(String neuronFilename) {
        Pattern p = Pattern.compile(".*?_(\\d+)\\.(\\w+)");
        Matcher m = p.matcher(neuronFilename);
        if (m.matches()) {
            String mipNum = m.group(1);
            try {
                return Integer.parseInt(mipNum);
            } catch (NumberFormatException e) {
                logger.warn("Error parsing neuron index from filename: {} - {}", neuronFilename, mipNum);
            }
        }
        return null;
    }

    private SampleNeuronSeparationArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(jacsServiceData.getArgsArray(), new SampleNeuronSeparationArgs());
    }

}

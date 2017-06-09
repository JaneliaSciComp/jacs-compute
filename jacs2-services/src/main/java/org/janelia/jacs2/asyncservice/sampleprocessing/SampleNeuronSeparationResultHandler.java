package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import org.janelia.it.jacs.model.domain.Reference;
import org.janelia.it.jacs.model.domain.ReverseReference;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.sample.NeuronFragment;
import org.janelia.it.jacs.model.domain.sample.NeuronSeparation;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationFiles;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class SampleNeuronSeparationResultHandler {

    private final SampleDataService sampleDataService;
    private final Logger logger;

    SampleNeuronSeparationResultHandler(SampleDataService sampleDataService, Logger logger) {
        this.sampleDataService = sampleDataService;
        this.logger = logger;
    }

    void updateSampleNeuronSeparationResult(Number sampleId, String subject,
                                            String sampleObjective,
                                            Number pipelineRunId, Number parentResultId,
                                            Number resultId, String resultName, NeuronSeparationFiles neuronSeparationFiles) {
        Sample sample = sampleDataService.getSampleById(subject, sampleId);
        Preconditions.checkArgument(sample != null, "Invalid sample ID");
        NeuronSeparation neuronSeparation = new NeuronSeparation();
        neuronSeparation.setId(resultId);
        neuronSeparation.setName(resultName);
        neuronSeparation.setFilepath(neuronSeparationFiles.getResultDir());
        neuronSeparationFiles.findSeparationResult()
                .ifPresent(srf -> neuronSeparation.setFileName(FileType.NeuronSeparatorResult, srf));
        neuronSeparation.setFileName(FileType.FastStack, neuronSeparationFiles.getConsolidatedSignalMovieResult());
        ReverseReference fragmentsReference = new ReverseReference();
        fragmentsReference.setReferringClassName(NeuronFragment.class.getSimpleName());
        fragmentsReference.setReferenceAttr("separationId");
        fragmentsReference.setReferenceId(neuronSeparation.getId());
        fragmentsReference.setCount((long) neuronSeparationFiles.getNeurons().size());
        neuronSeparation.setFragmentsReference(fragmentsReference);

        List<NeuronFragment> neuronFragmentList = new ArrayList<>();
        for (String neuron : Ordering.natural().sortedCopy(neuronSeparationFiles.getNeurons())) {
            Integer neuronIndex = getNeuronIndex(neuron);
            NeuronFragment neuronFragment = new NeuronFragment();
            neuronFragment.setOwnerKey(sample.getOwnerKey());
            neuronFragment.setReaders(sample.getReaders());
            neuronFragment.setWriters(sample.getWriters());
            neuronFragment.setName("Neuron Fragment " + neuronIndex);
            neuronFragment.setNumber(neuronIndex);
            neuronFragment.setSample(Reference.createFor(sample));
            neuronFragment.setSeparationId(resultId);
            neuronFragment.setFilepath(neuronSeparationFiles.getResultDir());
            neuronFragment.setFileName(FileType.SignalMip, neuron);
            Optional<String> neuronMask = neuronSeparationFiles.getNeuronMask(neuronIndex);
            if (neuronMask.isPresent()) {
                neuronFragment.setFileName(FileType.MaskFile, neuronMask.get());
            }
            Optional<String> neuronChan = neuronSeparationFiles.getNeuronChan(neuronIndex);
            if (neuronChan.isPresent()) {
                neuronFragment.setFileName(FileType.ChanFile, neuronChan.get());
            }
            neuronFragmentList.add(neuronFragment);
        }
        sampleDataService.addSampleObjectivePipelineRunResult(sample, sampleObjective, pipelineRunId, parentResultId, neuronSeparation);
        sampleDataService.createNeuronFragments(neuronFragmentList);

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
                throw e;
            }
        }
        return null;
    }

}

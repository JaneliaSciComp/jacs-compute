package org.janelia.jacs2.asyncservice.alignservices;

import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationFiles;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;

import java.util.List;

public interface AlignmentServiceBuilder {
    boolean supports(String algorithm);
    List<AlignmentServiceParams> getAlignmentServicesArgs(Sample sample, String alignmentAlgorithm, String sampleDataRootDir, List<SampleProcessorResult> sampleProcessorResults, List<NeuronSeparationFiles> neuronSeparationResults);
}

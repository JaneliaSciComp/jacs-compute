package org.janelia.jacs2.asyncservice.alignservices;

import java.nio.file.Path;
import java.util.List;

import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationFiles;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;
import org.janelia.model.jacs2.domain.sample.Sample;

public interface AlignmentServiceBuilder {
    boolean supports(String algorithm);
    List<AlignmentServiceParams> getAlignmentServicesArgs(Sample sample, String alignmentAlgorithm, String alignmentResultName, Path sampleDataRootDir, List<SampleProcessorResult> sampleProcessorResults, List<NeuronSeparationFiles> neuronSeparationResults);
}

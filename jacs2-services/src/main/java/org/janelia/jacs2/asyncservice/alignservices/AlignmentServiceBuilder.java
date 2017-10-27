package org.janelia.jacs2.asyncservice.alignservices;

import org.janelia.model.jacs2.domain.sample.Sample;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationFiles;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;

import java.nio.file.Path;
import java.util.List;

public interface AlignmentServiceBuilder {
    boolean supports(String algorithm);
    List<AlignmentServiceParams> getAlignmentServicesArgs(Sample sample, String alignmentAlgorithm, String alignmentResultName, Path sampleDataRootDir, List<SampleProcessorResult> sampleProcessorResults, List<NeuronSeparationFiles> neuronSeparationResults);
}

package org.janelia.jacs2.asyncservice.alignservices;

import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationFiles;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;

import java.util.List;

public class AlignmentServiceParams {

    private final SampleProcessorResult sampleProcessorResult;
    private final NeuronSeparationFiles neuronSeparationFiles;
    private final List<ServiceArg> alignmentServiceArgs;

    public AlignmentServiceParams(SampleProcessorResult sampleProcessorResult, NeuronSeparationFiles neuronSeparationFiles, List<ServiceArg> alignmentServiceArgs) {
        this.sampleProcessorResult = sampleProcessorResult;
        this.neuronSeparationFiles = neuronSeparationFiles;
        this.alignmentServiceArgs = alignmentServiceArgs;
    }

    public SampleProcessorResult getSampleProcessorResult() {
        return sampleProcessorResult;
    }

    public NeuronSeparationFiles getNeuronSeparationFiles() {
        return neuronSeparationFiles;
    }

    public List<ServiceArg> getAlignmentServiceArgs() {
        return alignmentServiceArgs;
    }
}

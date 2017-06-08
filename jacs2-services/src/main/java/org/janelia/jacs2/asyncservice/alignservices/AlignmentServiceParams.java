package org.janelia.jacs2.asyncservice.alignservices;

import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationFiles;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;

import java.util.List;

public class AlignmentServiceParams {

    private final String alignmentResultName;
    private final SampleProcessorResult sampleProcessorResult;
    private final NeuronSeparationFiles neuronSeparationFiles;
    private final List<ServiceArg> alignmentServiceArgs;

    public AlignmentServiceParams(String alignmentResultName, SampleProcessorResult sampleProcessorResult, NeuronSeparationFiles neuronSeparationFiles, List<ServiceArg> alignmentServiceArgs) {
        this.alignmentResultName = alignmentResultName;
        this.sampleProcessorResult = sampleProcessorResult;
        this.neuronSeparationFiles = neuronSeparationFiles;
        this.alignmentServiceArgs = alignmentServiceArgs;
    }

    public String getAlignmentResultName() {
        return alignmentResultName;
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

    public ServiceArg[] getAlignmentServiceArgsArray() {
        return alignmentServiceArgs.toArray(new ServiceArg[alignmentServiceArgs.size()]);
    }
}

package org.janelia.jacs2.asyncservice.alignservices;

import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationFiles;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AlignmentServiceParams {

    private final String alignmentResultName;
    private final String alignmentAlgorithm;
    private final SampleProcessorResult sampleProcessorResult;
    private final NeuronSeparationFiles neuronSeparationFiles;
    private final List<ServiceArg> alignmentServiceArgs;
    private final Map<String, String> resources = new LinkedHashMap<>();

    public AlignmentServiceParams(String alignmentResultName, String alignmentAlgorithm, SampleProcessorResult sampleProcessorResult, NeuronSeparationFiles neuronSeparationFiles, List<ServiceArg> alignmentServiceArgs) {
        this.alignmentResultName = alignmentResultName;
        this.alignmentAlgorithm = alignmentAlgorithm;
        this.sampleProcessorResult = sampleProcessorResult;
        this.neuronSeparationFiles = neuronSeparationFiles;
        this.alignmentServiceArgs = alignmentServiceArgs;
    }

    public String getAlignmentResultName() {
        return alignmentResultName;
    }

    public String getAlignmentAlgorithm() {
        return alignmentAlgorithm;
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

    public Map<String, String> getResources() {
        return resources;
    }
}

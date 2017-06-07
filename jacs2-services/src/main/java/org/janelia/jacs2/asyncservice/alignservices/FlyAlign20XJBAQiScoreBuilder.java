package org.janelia.jacs2.asyncservice.alignservices;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationFiles;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;
import org.janelia.jacs2.asyncservice.utils.FileUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class FlyAlign20XJBAQiScoreBuilder implements AlignmentServiceBuilder {

    @Override
    public boolean supports(String algorithm) {
        return "flyalign20x_JBA_Qiscore".equals(algorithm);
    }

    @Override
    public List<List<ServiceArg>> getAlignmentServicesArgs(String alignmentAlgorithm,
                                                           String sampleDataRootDir,
                                                           List<SampleProcessorResult> sampleProcessorResults,
                                                           List<NeuronSeparationFiles> neuronSeparationResults) {
        List<List<ServiceArg>> alignmentServicesArgs = new ArrayList<>();
        int resultIndex = 0;
        for (SampleProcessorResult sampleProcessorResult : sampleProcessorResults) {
            if (neuronSeparationResults.size() < resultIndex) {
                throw new IllegalStateException("The number of sampleProcessor results and neuron separation results differ: " + sampleProcessorResults + ", " + neuronSeparationResults);
            }
            NeuronSeparationFiles neuronSeparationFiles = neuronSeparationResults.get(resultIndex);
            alignmentServicesArgs.add(ImmutableList.of(
                    new ServiceArg("-i1File", sampleProcessorResult.getAreaFile()),
                    new ServiceArg("-i1Channels", sampleProcessorResult.getNumChannels()),
                    new ServiceArg("-i1Ref", sampleProcessorResult.getReferenceChannelNumber()),
                    new ServiceArg("-i1Res", sampleProcessorResult.getOpticalResolution()),
                    new ServiceArg("-i1Dims", sampleProcessorResult.getImageSize()),
                    new ServiceArg("-o", getAlignmentOutputDir(sampleDataRootDir, "Alignment", sampleProcessorResult.getResultId(), sampleProcessorResults.size(), sampleProcessorResult.getArea(), resultIndex++).toString()),
                    new ServiceArg("-i1Neurons", neuronSeparationFiles.getConsolidatedLabel()),
                    new ServiceArg("-alignmentAlgorithm", alignmentAlgorithm)
            ));
        }
        return alignmentServicesArgs;
    }

    private Path getAlignmentOutputDir(String sampleDataRootDir, String subDir, Number parentResultId, int nAreas, String area, int resultIndex) {
        Path alignmentOutputDir;
        if (StringUtils.isNotBlank(area)) {
            alignmentOutputDir = Paths.get(sampleDataRootDir)
                    .resolve(FileUtils.getDataPath(subDir, parentResultId))
                    .resolve(area);
        } else if (nAreas > 1) {
            alignmentOutputDir = Paths.get(sampleDataRootDir)
                    .resolve(FileUtils.getDataPath(subDir, parentResultId.toString()))
                    .resolve("area" + resultIndex + 1);
        } else {
            alignmentOutputDir = Paths.get(sampleDataRootDir)
                    .resolve(FileUtils.getDataPath(subDir, parentResultId.toString()));
        }
        return alignmentOutputDir;
    }

}

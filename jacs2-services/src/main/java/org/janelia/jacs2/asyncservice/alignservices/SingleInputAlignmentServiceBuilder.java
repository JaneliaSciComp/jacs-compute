package org.janelia.jacs2.asyncservice.alignservices;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.neuronservices.NeuronSeparationFiles;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;
import org.janelia.jacs2.asyncservice.utils.FileUtils;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SingleInputAlignmentServiceBuilder implements AlignmentServiceBuilder {

    private static final long LARGE_FILE_SIZE_THRESHOLD_UNCOMPRESSED = (4L * 1024L * 1024L * 1024L);
    private static final long LARGE_FILE_SIZE_THRESHOLD_COMPRESSED = (2L * 1024L * 1024L * 1024L);

    private static final Set<String> SUPPORTED_ALGORITHMS = ImmutableSet.of(
            "brainalign40xMCFO_INT",
            "brainalign40x_512px_INT",
            "brainalign63xFlpout_1024px_INT",
            "brainalign63xFlpout_512px_INT",
            "brainalign63xLexAGAL4_1024px_INT",
            "brainalign63xLexAGAL4_512px_INT",
            "brainalignPolarity20xBrainVNCRescale",
            "brainalignPolarity20xBrainVNCRescale_dpx",
            "brainalignPolarityPair_1024px_INT",
            "brainalignPolarityPair_dpx_1024px_INT",
            "brainalignPolarityPair_ds_dpx_1024px_INT_v3",
            "flyalign20xVNC_CMTK",
            "flyalign20x_INT",
            "flyalign20x_JBA_Qiscore",
            "flyalign20x_dpx_1024px_INT",
            "flybrainaligner20x_ditp_JBA",
            "flyleftopticlobealign_512px_INT_MT",
            "flyrightopticlobealign_512px_INT_MT"
    );

    @Override
    public boolean supports(String algorithm) {
        return SUPPORTED_ALGORITHMS.contains(algorithm);
    }

    @Override
    public List<AlignmentServiceParams> getAlignmentServicesArgs(Sample sample,
                                                                 String alignmentAlgorithm,
                                                                 String alignmentResultName,
                                                                 String sampleDataRootDir,
                                                                 List<SampleProcessorResult> sampleProcessorResults,
                                                                 List<NeuronSeparationFiles> neuronSeparationResults) {
        List<AlignmentServiceParams> alignmentServicesParams = new ArrayList<>();
        int resultIndex = 0;
        for (SampleProcessorResult sampleProcessorResult : sampleProcessorResults) {
            if (neuronSeparationResults.size() < resultIndex) {
                throw new IllegalStateException("The number of sampleProcessor results and neuron separation results differ: " + sampleProcessorResults + ", " + neuronSeparationResults);
            }
            NeuronSeparationFiles neuronSeparationFiles = neuronSeparationResults.get(resultIndex);
            AlignmentServiceParams alignmentServiceParams = new AlignmentServiceParams(
                    alignmentAlgorithm,
                    alignmentResultName,
                    sampleProcessorResult,
                    neuronSeparationFiles,
                    ImmutableList.of(
                            new ServiceArg("-i1File", sampleProcessorResult.getAreaFile()),
                            new ServiceArg("-i1Channels", sampleProcessorResult.getNumChannels()),
                            new ServiceArg("-i1Ref", sampleProcessorResult.getReferenceChannelNumber()),
                            new ServiceArg("-i1Res", sampleProcessorResult.getOpticalResolution()),
                            new ServiceArg("-i1Dims", sampleProcessorResult.getImageSize()),
                            new ServiceArg("-o", getAlignmentOutputDir(sampleDataRootDir, "Alignment", sampleProcessorResult.getResultId(), sampleProcessorResults.size(), sampleProcessorResult.getArea(), resultIndex++).toString()),
                            new ServiceArg("-i1Neurons", neuronSeparationFiles.getConsolidatedLabelPath().toString()),
                            new ServiceArg("-gender", sample.getGender()),
                            new ServiceArg("-mountingProtocol", sample.getMountingProtocol()),
                            new ServiceArg("-alignmentAlgorithm", alignmentAlgorithm)
                    )
            );
            ProcessorHelper.setRequiredMemoryInGB(alignmentServiceParams.getResources(), getRequiredMemoryInGB(sampleProcessorResult.getAreaFile()));
            alignmentServicesParams.add(alignmentServiceParams);
        }
        return alignmentServicesParams;
    }

    private int getRequiredMemoryInGB(String areaFileName) {
        File areaFile = new File(areaFileName);
        long fileSize = areaFile.length();
        if ((areaFileName.endsWith("raw") && fileSize > LARGE_FILE_SIZE_THRESHOLD_UNCOMPRESSED) ||
                (areaFileName.endsWith("pbd") && fileSize > LARGE_FILE_SIZE_THRESHOLD_COMPRESSED)) {
            return 96;
        } else {
            return 24;
        }
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

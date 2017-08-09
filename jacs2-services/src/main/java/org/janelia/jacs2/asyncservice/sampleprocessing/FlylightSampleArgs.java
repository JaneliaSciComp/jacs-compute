package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;

class FlylightSampleArgs extends SampleServiceArgs {
    @Parameter(names = "-sampleResultsName", description = "The name for the sample results", required = false)
    String sampleResultsName;
    @Parameter(names = "-sampleProcessName", description = "The name for the sample process", required = false)
    String sampleProcessName;
    @Parameter(names = "-imageType", description = "Image type: screen, polarity, MCFO", required = false)
    String imageType;
    @Parameter(names = "-mergeAlgorithm", description = "Merge algorithm", required = false)
    String mergeAlgorithm;
    @Parameter(names = "-channelDyeSpec", description = "Channel dye spec", required = false)
    String channelDyeSpec;
    @Parameter(names = "-outputChannelOrder", description = "Output channel order", required = false)
    String outputChannelOrder;
    @Parameter(names = "-basicMipMapsOptions", description = "Basic MIPS and Movies Options", required = false)
    String basicMipMapsOptions = "mips:movies:legends:bcomp";
    @Parameter(names = "-postProcessingMipMapsOptions", description = "Post processing MIPS and Movies Options", required = false)
    String postProcessingMipMapsOptions = "mips:movies:legends:hist";
    @Parameter(names = "-defaultPostProcessingColorSpec", description = "Default post processing color spec", required = false)
    String defaultPostProcessingColorSpec;
    @Parameter(names = "-skipSummary", description = "If set do not run LSM summary", required = false)
    boolean skipSummary;
    @Parameter(names = "-montageMipMaps", description = "If set montage the mipmaps", required = false)
    boolean montageMipMaps;
    @Parameter(names = "-persistResults", description = "If specified it generates the mips", required = false)
    boolean persistResults;
    @Parameter(names = "-runNeuronSeparationAfterSampleProcessing", description = "If specified it runs the neuron separation after sample processing", required = false)
    boolean runNeuronSeparationAfterSampleProcessing;
    @Parameter(names = "-alignmentAlgorithm", description = "Specifies the alignment algorithms to be run", required = false)
    List<String> alignmentAlgorithms = new ArrayList<>();
    @Parameter(names = "-alignmentResultName", description = "Specifies the alignment result name", required = false)
    List<String> alignmentResultNames = new ArrayList<>();
    @Parameter(names = {"-compressTypes"}, description = "Result file types to be compressed", required = false)
    List<String> resultFileTypesToBeCompressed = new ArrayList<>();
    @Parameter(names = {"-compressedFileType"}, description = "Compressed file type", required = false)
    String compressedFileType;
    @Parameter(names = "-keepUncompressedResults", description = "If set it keeps uncompressed results in case any compression is performed ", required = false)
    boolean keepUncompressedResults;
}

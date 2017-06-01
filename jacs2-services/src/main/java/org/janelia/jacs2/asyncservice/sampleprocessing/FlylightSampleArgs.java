package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;

class FlylightSampleArgs extends SampleServiceArgs {
    @Parameter(names = "-mergeAlgorithm", description = "Merge algorithm", required = false)
    String mergeAlgorithm;
    @Parameter(names = "-channelDyeSpec", description = "Channel dye spec", required = false)
    String channelDyeSpec;
    @Parameter(names = "-outputChannelOrder", description = "Output channel order", required = false)
    String outputChannelOrder;
    @Parameter(names = "-distortionCorrection", description = "If specified apply distortion correction", required = false)
    boolean applyDistortionCorrection;
    @Parameter(names = "-basicMipMapsOptions", description = "Basic MIPS and Movies Options", required = false)
    String basicMipMapsOptions = "mips:movies:legends:bcomp";
    @Parameter(names = "-montageMipMaps", description = "If set montage the mipmaps", required = false)
    boolean montageMipMaps;
    @Parameter(names = "-persistResults", description = "If specified it generates the mips", required = false)
    boolean persistResults;
}

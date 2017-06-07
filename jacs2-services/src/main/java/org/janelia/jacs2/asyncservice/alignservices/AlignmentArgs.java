package org.janelia.jacs2.asyncservice.alignservices;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;

class AlignmentArgs extends ServiceArgs {
    @Parameter(names = {"-alignmentAlgorithm"}, description = "The name of the alignment algorithm (or alignment script)", required = false)
    String alignmentAlgorithm;
    @Parameter(names = {"-nthreads"}, description = "Number of ITK threads")
    Integer numThreads = 16;
    @Parameter(names = {"-i", "-input1"},
            description = "The alignment first input which is a comma separated list of the image file name, input channels, reference, resolution and dimensions",
            required = false)
    String input1;
    @Parameter(names = "-i1File", description = "The name of the first input file", required = false)
    String input1File;
    @Parameter(names = "-i1Channels", description = "The number of channels of the first input file", required = false)
    int input1Channels;
    @Parameter(names = "-i1Ref", description = "The one based reference channel for the first input file", required = false)
    int input1Ref;
    @Parameter(names = "-i1Res", description = "The resolution of the first input file", required = false)
    String input1Res;
    @Parameter(names = "-i1Dims", description = "The dimensions of the first input file", required = false)
    String input1Dims;
    @Parameter(names = {"-e", "-i1Neurons"}, description = "Input1 neurons file", required = false)
    String input1Neurons;
    @Parameter(names = {"-j", "-input2"},
            description = "The alignment second input which is a comma separated list of the image file name, input channels, reference, resolution and dimensions",
            required = false)
    String input2;
    @Parameter(names = "-i2File", description = "The name of the second input file", required = false)
    String input2File;
    @Parameter(names = "-i2Channels", description = "The channels of the second input file", required = false)
    int input2Channels;
    @Parameter(names = "-i2Ref", description = "The reference for the second input file", required = false)
    int input2Ref;
    @Parameter(names = "-i2Res", description = "The resolution of the second input file", required = false)
    String input2Res;
    @Parameter(names = "-i2Dims", description = "The dimensions of the second input file", required = false)
    String input2Dims;
    @Parameter(names = {"-f", "-i2Neurons"}, description = "Input2 neurons file", required = false)
    String input2Neurons;
    @Parameter(names = {"-s", "-step"}, description = "Step", required = false)
    String step;
    @Parameter(names = {"-m", "-mp", "-mountingProtocol"}, description = "Mounting protocol", required = false)
    String mountingProtocol;
    @Parameter(names = {"-g", "-gender"}, description = "Gender", required = false)
    String gender;
    @Parameter(names = {"-alignmentSpace"}, description = "Alignment space name", required = false)
    String alignmentSpace = "";
    @Parameter(names = {"-targetTemplate"}, description = "Target template", required = false)
    String targetTemplate;
    @Parameter(names = {"-targetExtTemplate"}, description = "Target EXT template", required = false)
    String targetExtTemplate;
    @Parameter(names = {"-z", "-zflip"}, arity = 0, description = "Z flip flag", required = false)
    boolean zFlip = false;
    @Parameter(names = "-fslOutputType", description = "FSL output type", required = false)
    String fslOutputType = "NIFTI_GZ";
    @Parameter(names = {"-o", "-w", "-outputDir"}, description = "Results directory", required = true)
    String outputDir;
}

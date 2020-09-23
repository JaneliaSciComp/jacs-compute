package org.janelia.jacs2.asyncservice.imagesearch;

import java.util.List;

import com.beust.jcommander.Parameter;

import org.janelia.jacs2.asyncservice.common.ServiceArgs;

class ColorDepthSearchArgs extends ServiceArgs {
    @Parameter(names = {"-masksFiles", "-inputMasks", "-inputFiles"},
            description = "List of mask files to be searched against the specified libraries",
            required = true)
    List<String> masksFiles;

    @Parameter(names = {"-targetsFiles", "-searchImageFile"}, description = "Filepath to one or more JSON files containing all MIPs to search")
    List<String> targetsFiles;

    @Parameter(names = {"-cdMatchesDir", "-od"}, description = "Color depth matches or results directory")
    String cdMatchesDir;

    @Parameter(names = {"-maskThreshold"}, description = "Mask thresholds", variableArity = true)
    Integer maskThreshold;

    @Parameter(names = {"-dataThreshold"}, description = "Data threshold")
    Integer dataThreshold;

    @Parameter(names = {"-pixColorFluctuation"}, description = "Pix Color Fluctuation, 1.18 per slice")
    Double pixColorFluctuation;

    @Parameter(names = {"-xyShift"}, description = "Number of pixels to try shifting in XY plane")
    Integer xyShift = 0;

    @Parameter(names = {"-mirrorMask"}, description = "Should the mask be mirrored across the Y axis?")
    Boolean mirrorMask = false;

    @Parameter(names = {"-pctPositivePixels"}, description = "% of Positive PX Threshold (0-100%)")
    Double pctPositivePixels;

    @Parameter(names = {"-negativeRadius"}, description = "Negative radius for the gradient score")
    Integer negativeRadius = 20;
}

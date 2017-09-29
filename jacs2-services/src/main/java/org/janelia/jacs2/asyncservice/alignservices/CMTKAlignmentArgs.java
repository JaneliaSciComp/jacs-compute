package org.janelia.jacs2.asyncservice.alignservices;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;

import java.util.ArrayList;
import java.util.List;

final class CMTKAlignmentArgs extends ServiceArgs {
    @Parameter(names = "-inputDir", description = "The input folder")
    String inputDir;
    @Parameter(names = "-inputImages", description = "The input NRRD image files")
    List<String> inputImageFileNames = new ArrayList<>();
    @Parameter(names = "-outputDir", description = "The output folder")
    String outputDir;
    @Parameter(names = "-template", description = "The alignment template", required = false)
    String template;
    @Parameter(names = "-a", description = "Run affine", arity = 1)
    Boolean runAffine = true;
    @Parameter(names = "-w", description = "Run warp", arity = 1)
    Boolean runWarp = true;
    @Parameter(names = "-r", description = "Channels to reformat", required = false)
    String reformattingChannels = "0102030405";
    @Parameter(names = "-X", description = "Exploration parameter", required = false)
    String exploration = "26";
    @Parameter(names = "-C", description = "Coarsest parameter", required = false)
    String coarsest = "8";
    @Parameter(names = "-G", description = "Grid spacing", required = false)
    String gridSpacing = "80";
    @Parameter(names = "-R", description = "Refine parameter", required = false)
    String refine = "4";
    @Parameter(names = "-A", description = "Affine transformation options", required = false)
    String affineOptions = "--accuracy 0.8";
    @Parameter(names = "-W", description = "Warp transformation options", required = false)
    String warpOptions = "--accuracy 0.8";
    @Parameter(names = {"-nthreads"}, description = "Number of ITK threads")
    Integer numThreads;

    int getNumThreads() {
        return numThreads == null ? 0 : numThreads;
    }
}

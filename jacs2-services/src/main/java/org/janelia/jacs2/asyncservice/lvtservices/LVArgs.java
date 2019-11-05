package org.janelia.jacs2.asyncservice.lvtservices;

import com.beust.jcommander.Parameter;

import org.janelia.jacs2.asyncservice.common.ServiceArgs;

/**
 * Large Volume Tool arguments.
 */
class LVArgs extends ServiceArgs {
    @Parameter(names = "-inputDir", description = "Input data directory", required = true)
    String inputDir;
    @Parameter(names = "-outputDir", description = "Output directory for octree", required = true)
    String outputDir;
    @Parameter(names = "-levels", description = "Number of octree levels")
    Integer levels = 3;
}

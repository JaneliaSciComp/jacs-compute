package org.janelia.jacs2.asyncservice.lvtservices;

import com.beust.jcommander.Parameter;

/**
 * Large Volume Tool arguments.
 */
class LVTArgs extends LVArgs {
    @Parameter(names = "-containerProcessor", description = "Container processor: docker or singularity")
    String containerProcessor;
    @Parameter(names = "-containerRuntimeArgs", description = "Processor specific runtime arguments")
    String containerRuntimeArgs;
    @Parameter(names = "-toolContainerImage", description = "Name of the container image for this LV tool")
    String toolContainerImage;
}

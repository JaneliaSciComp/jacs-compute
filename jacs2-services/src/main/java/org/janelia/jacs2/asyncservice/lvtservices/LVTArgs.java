package org.janelia.jacs2.asyncservice.lvtservices;

import com.beust.jcommander.Parameter;

/**
 * Large Volume Tool arguments.
 */
class LVTArgs extends LVArgs {
    @Parameter(names = "-toolContainerImage", description = "Name of the container image for this LV tool")
    String toolContainerImage;
}

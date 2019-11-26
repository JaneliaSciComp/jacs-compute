package org.janelia.jacs2.asyncservice.containerizedservices;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;

abstract class AbstractContainerArgs extends ServiceArgs {
    @Parameter(names = "-containerLocation", description = "Container image location. This can be either a registry image name or a local image file", required = true)
    String containerLocation;
    @Parameter(names = "-runtime", description = "Container runtime binary")
    String runtime;

    AbstractContainerArgs(String serviceDescription) {
        super(serviceDescription);
    }
}

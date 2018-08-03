package org.janelia.jacs2.asyncservice.containerizedservices;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;

import java.util.List;

abstract class AbstractSingularityContainerArgs extends ServiceArgs {
    @Parameter(names = "-containerLocation", description = "Singularity container location", required = true)
    String containerLocation;
    @Parameter(names = "-singularityRuntime", description = "Singularity binary")
    String singularityRuntime;
    @Parameter(names = "-enableHttps", description = "Enable HTTPS for retrieving the container image")
    boolean enableHttps;
    @Parameter(names = "-containerImagesDir", description = "Local container images directory")
    String containerImagesDirectory;
    @Parameter(names = "-containerName", description = "Local container name")
    String containerName;

    AbstractSingularityContainerArgs(String serviceDescription) {
        super(serviceDescription);
    }

    boolean noHttps() {
        return !enableHttps;
    }
}

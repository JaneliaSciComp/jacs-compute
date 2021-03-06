package org.janelia.jacs2.asyncservice.containerizedservices;

import com.beust.jcommander.Parameter;

class PullSingularityContainerArgs extends AbstractContainerArgs {
    @Parameter(names = "-enableHttps", description = "Enable HTTPS for retrieving the container image")
    boolean enableHttps;
    @Parameter(names = "-containerImagesDir", description = "Local container images directory")
    String containerImagesDirectory;

    PullSingularityContainerArgs() {
        super("Service that pulls a singularity container image");
    }

    boolean noHttps() {
        return !enableHttps;
    }
}

package org.janelia.jacs2.asyncservice.containerizedservices;

import com.beust.jcommander.Parameter;

class SingularityContainerArgs extends SingularityRunContainerArgs {
    @Parameter(names = "-enableHttps", description = "Enable HTTPS for retrieving the container image")
    boolean enableHttps;
    @Parameter(names = "-containerImagesDir", description = "Local container images directory")
    String containerImagesDirectory;

    SingularityContainerArgs() {
        this("Service that pulls and runs a singularity container");
    }

    SingularityContainerArgs(String description) {
        super(description);
    }
}

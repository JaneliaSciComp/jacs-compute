package org.janelia.jacs2.asyncservice.containerizedservices;

import com.beust.jcommander.Parameter;

class SingularityRunContainerArgs extends RunContainerArgs {
    @Parameter(names = "-overlay", description = "Container overlay")
    String overlay;
    @Parameter(names = "-enableNV", description = "Enable NVidia support")
    boolean enableNV;
    @Parameter(names = "-initialPwd", description = "Initial working directory inside the container")
    String initialPwd;

    SingularityRunContainerArgs() {
        this("Service that runs a singularity container");
    }

    SingularityRunContainerArgs(String description) {
        super(description);
    }
}

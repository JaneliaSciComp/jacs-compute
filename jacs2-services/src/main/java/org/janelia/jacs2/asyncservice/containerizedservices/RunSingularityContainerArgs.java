package org.janelia.jacs2.asyncservice.containerizedservices;

import com.beust.jcommander.Parameter;

import java.util.List;

class RunSingularityContainerArgs extends AbstractSingularityContainerArgs {
    enum ContainerOperation {
        run,
        exec
    }

    @Parameter(names = "-op", description = "Singularity container operation {run (default) | exec}")
    ContainerOperation operation = ContainerOperation.run;
    @Parameter(names = "-appName", description = "Containerized application Name")
    String appName;
    @Parameter(names = "-bindPaths", description = "Container bind paths")
    List<String> bindPaths;
    @Parameter(names = "-overlay", description = "Container overlay")
    String overlay;
    @Parameter(names = "-enableNV", description = "Enable NVidia support")
    boolean enableNV;
    @Parameter(names = "-initialPwd", description = "Initial working directory inside the container")
    String initialPwd;
    @Parameter(names = "-appArgs", description = "Containerized application arguments")
    List<String> appArgs;

    RunSingularityContainerArgs() {
        super("Service that runs a singularity container");
    }
}

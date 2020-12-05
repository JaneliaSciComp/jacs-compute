package org.janelia.jacs2.asyncservice.spark;

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class SparkClusterArgs extends SparkArgs {
    @Parameter(names = {"-sparkJobId", "-sparkMasterJobId"}, description = "Spark master job ID")
    String sparkMasterJobId;
    @Parameter(names = "-sparkWorkerJobId", description = "Spark worker job ID")
    String sparkWorkerJobId;
    @Parameter(names = "-minSparkWorkers", description = "Minimum required spark workers")
    int minSparkWorkers = 0;

    SparkClusterArgs(String serviceDescription) {
        super(serviceDescription);
    }

    Long getSparkMasterJobId() {
        return Long.valueOf(sparkMasterJobId);
    }

    Long getSparkWorkerJobId() {
        return Long.valueOf(sparkWorkerJobId);
    }

}

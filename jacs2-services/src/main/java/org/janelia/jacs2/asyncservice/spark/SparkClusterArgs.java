package org.janelia.jacs2.asyncservice.spark;

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class SparkClusterArgs extends SparkArgs {
    @Parameter(names = "-sparkJobId", description = "Spark cluster ID")
    String sparkJobId;
    @Parameter(names = "-sparkWorkerJobIds", description = "Spark worker job IDs")
    List<String> sparkWorkerJobIds = new ArrayList<>();
    @Parameter(names = "-minSparkWorkers", description = "Minimum required spark workers")
    int minSparkWorkers = 0;
//    @Parameter(names = "-appLocation", description = "Spark application location", required = true)
//    String appLocation;
//    @Parameter(names = "-appEntryPoint", description = "Spark application entry point, i.e., java main class name")
//    String appEntryPoint;
//    @Parameter(names = "-appArgs", description = "Spark application arguments", splitter = ServiceArgSplitter.class)
//    List<String> appArgs = new ArrayList<>();

    SparkClusterArgs(String serviceDescription) {
        super(serviceDescription);
    }

    Long getSparkClusterJobId() {
        return Long.valueOf(sparkJobId);
    }

    List<Long> getSparkWorkerJobIds() {
        return sparkWorkerJobIds.stream().map(sId -> Long.valueOf(sId)).collect(Collectors.toList());
    }

}

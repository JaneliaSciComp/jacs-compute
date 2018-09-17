package org.janelia.jacs2.asyncservice.spark;

import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;

class SparkAppArgs extends SparkArgs {
    @Parameter(names = "-sparkJobId", description = "Spark cluster ID")
    Long sparkJobId;
    @Parameter(names = "-appLocation", description = "Spark application location", required = true)
    String appLocation;
    @Parameter(names = "-appEntryPoint", description = "Spark application entry point, i.e., java main class name")
    String appEntryPoint;
    @Parameter(names = "-appArgs", description = "Spark application arguments", splitter = ServiceArgSplitter.class)
    List<String> appArgs = new ArrayList<>();

    SparkAppArgs() {
        super("Spark application processor");
    }
}

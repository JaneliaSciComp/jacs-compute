package org.janelia.jacs2.asyncservice.spark;

import java.util.List;
import java.util.Map;

interface SparkDriverRunner<A extends SparkApp> {
    String DRIVER_OUTPUT_FILENAME = "sparkdriver.out";
    String DRIVER_ERROR_FILENAME = "sparkdriver.err";

    A startSparkApp(String appName,
                    SparkClusterInfo sparkClusterInfo,
                    String appResource,
                    String appEntryPoint,
                    List<String> appArgs,
                    String appOutputDir,
                    String appErrorDir,
                    Map<String, String> sparkAppResources);

}

package org.janelia.jacs2.asyncservice.spark;

import java.io.File;
import java.util.List;
import java.util.Map;

import com.google.common.base.Stopwatch;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LocalSparkDriverRunner implements SparkDriverRunner<LocalProcessSparkApp> {
    private static final Logger LOG = LoggerFactory.getLogger(LocalSparkDriverRunner.class);

    public LocalProcessSparkApp startSparkApp(String appName,
                                              SparkClusterInfo sparkClusterInfo,
                                              String appResource,
                                              String appEntryPoint,
                                              List<String> appArgs,
                                              String appOutputDir,
                                              String appErrorDir,
                                              Map<String, String> sparkAppResources) {
        LOG.info("Starting app {} on {} ({}, {}); appArgs: {}",
                appResource,
                sparkClusterInfo.getMasterURI(), sparkClusterInfo.getMasterJobId(), sparkClusterInfo.getWorkerJobId(),
                appArgs);
        LOG.info("Driver output: {}", appOutputDir);
        LOG.info("Driver error: {}", appErrorDir);
        SparkLauncher sparkLauncher = new SparkLauncher();
        File sparkErrorFile;
        if (StringUtils.isNotBlank(appErrorDir)) {
            sparkErrorFile = new File(appErrorDir, DRIVER_ERROR_FILENAME);
            sparkLauncher.redirectError(sparkErrorFile);
        } else {
            sparkErrorFile = null;
        }
        if (StringUtils.isNotBlank(appOutputDir)) {
            sparkLauncher.redirectOutput(new File(appOutputDir, DRIVER_OUTPUT_FILENAME));
        }
        String[] appArgsArray;
        if (CollectionUtils.isEmpty(appArgs)) {
            appArgsArray = new String[0];
        } else {
            appArgsArray = appArgs.toArray(new String[0]);
        }
        sparkLauncher
                .setAppName(appName)
                .setSparkHome(SparkAppResourceHelper.getSparkHome(sparkAppResources))
                .setMaster(sparkClusterInfo.getMasterURI())
                .setAppResource(appResource)
                .setConf("spark.ui.showConsoleProgress", "false") // The console progress bar screws up the STDOUT stream
                .addAppArgs(appArgsArray);
        if (StringUtils.isNotBlank(appEntryPoint)) {
            sparkLauncher.setMainClass(appEntryPoint);
        }
        String sparkDriverMemory = SparkAppResourceHelper.getSparkDriverMemory(sparkAppResources);
        if (StringUtils.isNotBlank(sparkDriverMemory)) {
            sparkLauncher.setConf(SparkLauncher.DRIVER_MEMORY, sparkDriverMemory);
        }
        int sparkWorkerCores = SparkAppResourceHelper.getSparkWorkerCores(sparkAppResources);
        sparkLauncher.setConf(SparkLauncher.EXECUTOR_CORES, "" + sparkWorkerCores);
        int sparkMemPerCoreInGB = SparkAppResourceHelper.getSparkWorkerMemoryPerCoreInGB(sparkAppResources);
        if (sparkMemPerCoreInGB > 0) {
            sparkLauncher.setConf(SparkLauncher.EXECUTOR_MEMORY, "" + sparkWorkerCores * sparkMemPerCoreInGB + "g");
        }
        int appDefinedParallelism = SparkAppResourceHelper.getSparkParallelism(sparkAppResources);
        int defaultSparkParallelism = 3 * sparkWorkerCores * SparkAppResourceHelper.getSparkWorkers(sparkAppResources);
        int sparkParallelism = appDefinedParallelism != 0 ? appDefinedParallelism : defaultSparkParallelism;
        if (sparkParallelism > 0) {
            // The default (4MB) open cost consolidates files into tiny partitions regardless of number of cores.
            // By forcing this parameter to zero, we can specify the exact parallelism we want.
            sparkLauncher
                    .setConf("spark.files.openCostInBytes", "0")
                    .setConf("spark.default.parallelism", "" + sparkParallelism);
        }
        StringBuilder sparkDriverJavaOptsBuilder = new StringBuilder();
        String sparkLogConfigFile = SparkAppResourceHelper.getSparkLogConfigFile(sparkAppResources);
        if (StringUtils.isNotBlank(sparkLogConfigFile)) {
            sparkLauncher.setConf(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS, "-Dlog4j.configuration=file://" + sparkLogConfigFile);
            sparkDriverJavaOptsBuilder.append("-Dlog4j.configuration=file://").append(SparkAppResourceHelper.getSparkLogConfigFile(sparkAppResources)).append(' ');
        }
        String appStackSize = SparkAppResourceHelper.getSparkAppStackSize(sparkAppResources);
        if (StringUtils.isNotBlank(appStackSize)) {
            sparkDriverJavaOptsBuilder.append("-Xss").append(appStackSize).append(' ');
        }
        String hadoopHome = SparkAppResourceHelper.getHadoopHome(sparkAppResources);
        if (StringUtils.isNotBlank(hadoopHome)) {
            sparkDriverJavaOptsBuilder.append("-Dhadoop.home.dir=").append(hadoopHome).append(' ');
            sparkDriverJavaOptsBuilder.append("-Djava.library.path=").append(hadoopHome).append("/lib/native ");
        }
        if (sparkDriverJavaOptsBuilder.length() > 0) {
            sparkLauncher.addSparkArg("--driver-java-options", sparkDriverJavaOptsBuilder.toString());
        }

        Stopwatch sparkAppWatch = Stopwatch.createStarted();
        SparkAppHandle.Listener completionListener = new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle sparkAppHandle) {
                SparkAppHandle.State sparkAppState = sparkAppHandle.getState();
                LOG.info("Spark application state changed: {}", sparkAppState);
                if (sparkAppState.isFinal()) {
                    long seconds = sparkAppWatch.elapsed().getSeconds();
                    LOG.info("Spark application completed after {} seconds [{}]", seconds, sparkAppState);
                    if (sparkAppState != SparkAppHandle.State.FINISHED) {
                        LOG.warn("Spark application {} finished with an error state {}", sparkAppHandle.getAppId(), sparkAppState);
                    }
                }
            }

            @Override
            public void infoChanged(SparkAppHandle sparkAppHandle) {
                LOG.info("Changed info for spark application {} (application running for {})", sparkAppHandle.getAppId(), sparkAppWatch.elapsed());
            }
        };

        try {
            SparkAppHandle sparkAppHandle = sparkLauncher.startApplication(completionListener);
            return new LocalProcessSparkApp(sparkAppHandle, sparkErrorFile != null ? sparkErrorFile.getAbsolutePath() : null);
        } catch (Exception e) {
            LOG.error("Error running spark application {} on {}", appResource, sparkClusterInfo, e);
            throw new IllegalStateException(e);
        }
    }

}

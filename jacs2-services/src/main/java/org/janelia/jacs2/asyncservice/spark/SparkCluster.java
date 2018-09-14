package org.janelia.jacs2.asyncservice.spark;

import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.janelia.cluster.JobInfo;
import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobStatus;
import org.janelia.cluster.lsf.LsfJobInfo;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.cluster.ComputeAccounting;
import org.janelia.jacs2.asyncservice.common.cluster.LsfParseUtils;
import org.janelia.jacs2.asyncservice.common.cluster.MonitoredJobManager;
import org.janelia.jacs2.cdi.qualifier.BoolPropertyValue;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.model.service.JacsJobInstanceInfo;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * Manages a single Spark cluster with N nodes. Takes care of starting and stopping the master/worker nodes,
 * as well as submitting Spark applications to run on the cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SparkCluster {

    private static final String DRIVER_OUTPUT_FILENAME = "sparkdriver.out";
    private static final String DRIVER_ERROR_FILENAME = "sparkdriver.err";

    private final ServiceComputationFactory computationFactory;
    private final JobManager jobMgr;
    private final Logger logger;
    private final Long jobId;
    private final String masterURI;
    private final String sparkHomeDir;
    private final String hadoopHomeDir;
    private final String sparkDriverMemory;
    private final String sparkExecutorMemory;
    private final int sparkExecutorCores;
    private final int defaultParallelism;
    private final String sparkLogConfigFile;

    private AtomicReference<SparkApp> currentSparkApp = new AtomicReference<>();

    SparkCluster(ServiceComputationFactory computationFactory,
                 JobManager jobMgr,
                 Long jobId,
                 String masterURI,
                 String sparkHomeDir,
                 String hadoopHomeDir,
                 String sparkDriverMemory,
                 String sparkExecutorMemory,
                 int sparkExecutorCores,
                 int defaultParallelism,
                 String sparkLogConfigFile,
                 Logger logger) {
        this.jobId = jobId;
        this.masterURI = masterURI;
        this.sparkHomeDir = sparkHomeDir;
        this.hadoopHomeDir = hadoopHomeDir;
        this.sparkDriverMemory = sparkDriverMemory;
        this.sparkExecutorMemory = sparkExecutorMemory;
        this.sparkExecutorCores = sparkExecutorCores;
        this.defaultParallelism = defaultParallelism;
        this.sparkLogConfigFile = sparkLogConfigFile;
        this.computationFactory = computationFactory;
        this.jobMgr = jobMgr;
        this.logger = logger;
    }

    public Long getJobId() {
        return jobId;
    }

    public String getMasterURI() {
        return masterURI;
    }

    /**
     * Run the default main class in the specified jar file on the currently running cluster.
     * @param appResource absolute path to a jar file containing the app with dependencies or a python script
     * @param appParallelism
     * @param appArgs
     * @return
     * @throws Exception
     */
    public ServiceComputation<SparkApp> runApp(String appResource,
                                               int appParallelism,
                                               String appOutputDir,
                                               String appErrorDir,
                                               String... appArgs) {
        return runApp(appResource, null, appParallelism, appOutputDir, appErrorDir, appArgs);
    }

    /**
     * Run the default main class in the specified jar file on the currently running cluster.
     * @param appResource absolute path to a jar file containing the app with dependencies or a python script
     * @param appEntryPoint
     * @param appParallelism
     * @param appOutputDir
     * @param appErrorDir
     * @param appArgs
     * @return
     * @throws Exception
     */
    public ServiceComputation<SparkApp> runApp(String appResource,
                                               String appEntryPoint,
                                               int appParallelism,
                                               String appOutputDir,
                                               String appErrorDir,
                                               List<String> appArgs) {
        String[] appArgsArr = appArgs.toArray(new String[0]);
        return runApp(appResource, appEntryPoint, appParallelism, appOutputDir, appErrorDir, appArgsArr);
    }

    /**
     * Run the given main class in the specified jar file on the currently running cluster.
     * If the cluster is not ready an IllegalStateException will be thrown. Command line arguments can be passed
     * through via appArgs arguments.
     * @param appResource absolute path to a jar file containing the app with dependencies or a python script
     * @param appEntryPoint application entry point (main class for a java application)
     * @param appParallelism
     * @param appOutputDir
     * @param appErrorDir
     * @param appArgs
     * @return
     * @throws Exception
     */
    private ServiceComputation<SparkApp> runApp(String appResource,
                                                String appEntryPoint,
                                                int appParallelism,
                                                String appOutputDir,
                                                String appErrorDir,
                                                String... appArgs) {

        int parallelism = appParallelism > 0 ? appParallelism : defaultParallelism;
        logger.info("Starting app {} on {} ({}); " +
                        "driver output: {}, " +
                        "driver error: {}, " +
                        "sparkExecutorMemory: {}, " +
                        "sparkExecutorCores: {}, " +
                        "parallelism: {}, " +
                        "appArgs: {}",
                appResource, masterURI, jobId,
                appOutputDir,
                appErrorDir,
                sparkExecutorMemory,
                sparkExecutorCores,
                parallelism,
                Arrays.asList(appArgs));
        SparkLauncher sparkLauncher = new SparkLauncher();

        if (StringUtils.isNotBlank(appOutputDir)) {
            sparkLauncher.redirectOutput(new File(appOutputDir, DRIVER_OUTPUT_FILENAME));
        }
        if (StringUtils.isNotBlank(appErrorDir)) {
            sparkLauncher.redirectOutput(new File(appErrorDir, DRIVER_ERROR_FILENAME));
        }

        sparkLauncher.setAppResource(appResource)
                .setMaster(masterURI)
                .setSparkHome(sparkHomeDir)
                .setConf("spark.ui.showConsoleProgress", "false") // The console progress bar screws up the STDOUT stream
                .setConf(SparkLauncher.DRIVER_MEMORY, sparkDriverMemory)
                .setConf(SparkLauncher.EXECUTOR_MEMORY, sparkExecutorMemory)
                .setConf(SparkLauncher.EXECUTOR_CORES, "" + sparkExecutorCores)
                // The default (4MB) open cost consolidates files into tiny partitions regardless of number of cores.
                // By forcing this parameter to zero, we can specify the exact parallelism we want.
                .setConf("spark.files.openCostInBytes", "0")
                .setConf("spark.default.parallelism", "" + parallelism)
                .setConf(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS, "-Dlog4j.configuration=file://" + sparkLogConfigFile)
                .addSparkArg("--driver-java-options",
                        "-Dlog4j.configuration=file://" + sparkLogConfigFile +
                                " -Dhadoop.home.dir=" + hadoopHomeDir +
                                " -Djava.library.path=" + hadoopHomeDir +"/lib/native"
                )
                .addAppArgs(appArgs);

        if (StringUtils.isNotBlank(appEntryPoint)) {
            sparkLauncher.setMainClass(appEntryPoint);
        }

        try {
            ServiceComputation<SparkApp> sparkComputation = computationFactory.newComputation();

            Stopwatch sparkAppWatch = Stopwatch.createStarted();

            SparkApp sparkApp = new SparkApp(this, appOutputDir, appErrorDir);

            SparkAppHandle.Listener completionListener = new SparkAppHandle.Listener() {

                @Override
                public void stateChanged(SparkAppHandle sparkAppHandle) {
                    SparkAppHandle.State sparkAppState = sparkAppHandle.getState();
                    logger.info("Spark application state changed: {}", sparkAppState);
                    if (sparkAppState.isFinal()) {
                        long seconds = sparkAppWatch.stop().elapsed().getSeconds();
                        logger.info("Spark application completed after {} seconds [{}]", seconds, sparkAppState);
                        if (sparkAppState == SparkAppHandle.State.FINISHED) {
                            sparkComputation.supply(() -> sparkApp);
                        } else {
                            sparkComputation.supply(() -> {
                                throw new IllegalStateException("Spark application finished with an error state: " + sparkAppState);
                            });
                        }
                    }

                }

                @Override
                public void infoChanged(SparkAppHandle sparkAppHandle) {
                    logger.info("Changed info for spark application {} (application running for {})", sparkAppHandle.getAppId(), sparkAppWatch.elapsed());
                }
            };

            SparkAppHandle sparkAppHandle = sparkLauncher.startApplication(completionListener);
            sparkApp.updateHandle(sparkAppHandle);

            updateCurrentApp(sparkApp);

            return sparkComputation;
        } catch (Exception e) {
            logger.error("Error running application {} using {}", appResource, jobId, e);
            return computationFactory.newFailedComputation(e);
        }
    }

    private SparkApp updateCurrentApp(SparkApp newSparkApp) {
        return currentSparkApp.getAndUpdate(anyPrevSparkApp -> newSparkApp);
    }

    /**
     * Stop the cluster by killing the job. This must always be called before this job is completed.
     */
    public void stopCluster() {
        logger.info("Stopping Spark cluster {}", jobId);
        try {
            SparkApp sparkApp = updateCurrentApp(null);
            if (sparkApp != null && !sparkApp.isDone()) {
                logger.warn("Force killing Spark application {} running on cluster {}", sparkApp.getAppId(), jobId);
                sparkApp.kill();
            }
            jobMgr.killJob(jobId);
        } catch (Exception e) {
            logger.error("Error stopping Spark cluster {}", jobId, e);
        }
    }

}

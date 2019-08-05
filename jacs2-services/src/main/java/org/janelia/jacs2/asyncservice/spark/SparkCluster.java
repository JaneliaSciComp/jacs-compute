package org.janelia.jacs2.asyncservice.spark;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.janelia.cluster.JobManager;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.slf4j.Logger;

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
    private final Long masterJobId;
    private final List<Long> workerJobIds;
    private final int minRequiredWorkers;
    private final String masterURI;
    private final String sparkHomeDir;
    private final String hadoopHomeDir;
    private final String sparkDriverMemory;
    private final String sparkExecutorMemory;
    private final int coresPerSparkExecutor;
    private final int defaultParallelism;
    private final String sparkLogConfigFile;

    private AtomicReference<SparkApp> currentSparkApp = new AtomicReference<>();

    SparkCluster(ServiceComputationFactory computationFactory,
                 JobManager jobMgr,
                 Long masterJobId,
                 List<Long> workerJobIds,
                 int minRequiredWorkers,
                 String masterURI,
                 String sparkHomeDir,
                 String hadoopHomeDir,
                 String sparkDriverMemory,
                 String sparkExecutorMemory,
                 int coresPerSparkExecutor,
                 int defaultParallelism,
                 String sparkLogConfigFile,
                 Logger logger) {
        this.masterJobId = masterJobId;
        this.workerJobIds = workerJobIds;
        this.masterURI = masterURI;
        this.minRequiredWorkers = minRequiredWorkers;
        this.sparkHomeDir = sparkHomeDir;
        this.hadoopHomeDir = hadoopHomeDir;
        this.sparkDriverMemory = sparkDriverMemory;
        this.sparkExecutorMemory = sparkExecutorMemory;
        this.coresPerSparkExecutor = coresPerSparkExecutor;
        this.defaultParallelism = defaultParallelism;
        this.sparkLogConfigFile = sparkLogConfigFile;
        this.computationFactory = computationFactory;
        this.jobMgr = jobMgr;
        this.logger = logger;
    }

    public Long getMasterJobId() {
        return masterJobId;
    }

    List<Long> getWorkerJobIds() {
        return workerJobIds;
    }

    public String getMasterURI() {
        return masterURI;
    }

    int getMinRequiredWorkers() {
        return minRequiredWorkers;
    }

    String getSparkDriverMemory() {
        return sparkDriverMemory;
    }

    String getSparkExecutorMemory() {
        return sparkExecutorMemory;
    }

    int getDefaultParallelism() {
        return defaultParallelism;
    }

    String getSparkLogConfigFile() {
        return sparkLogConfigFile;
    }

    /**
     * Run the default main class in the specified jar file on the currently running cluster.
     * @param appResource absolute path to a jar file containing the app with dependencies or a python script
     * @param appEntryPoint nullable entry point - application main class
     * @param appParallelism
     * @param appOutputDir
     * @param appErrorDir
     * @param appArgs
     * @return
     * @throws Exception
     */
    public ServiceComputation<SparkApp> runApp(String appResource,
                                               @Nullable String appEntryPoint,
                                               int appParallelism,
                                               String appOutputDir,
                                               String appErrorDir,
                                               Long appIntervalCheck,
                                               Long appTimeout,
                                               List<String> appArgs) {
        String[] appArgsArr = appArgs.toArray(new String[0]);
        return runApp(appResource, appEntryPoint, appParallelism, appOutputDir, appErrorDir, appIntervalCheck, appTimeout, appArgsArr);
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
                                                Long appIntervalCheck,
                                                Long appTimeout,
                                                String... appArgs) {

        int parallelism = appParallelism > 0 ? appParallelism : defaultParallelism;
        logger.info("Starting app {} on {} ({}); " +
                        "sparkExecutorMemory: {}, " +
                        "sparkExecutorCores: {}, " +
                        "parallelism: {}, " +
                        "appArgs: {}",
                appResource, masterURI, masterJobId,
                sparkExecutorMemory,
                coresPerSparkExecutor,
                parallelism,
                Arrays.asList(appArgs));
        logger.info("Driver output: {}", appOutputDir);
        logger.info("Driver error: {}", appErrorDir);
        SparkLauncher sparkLauncher = new SparkLauncher();

        File sparkOutputFile;
        if (StringUtils.isNotBlank(appOutputDir)) {
            sparkOutputFile = new File(appOutputDir, DRIVER_OUTPUT_FILENAME);
            sparkLauncher.redirectOutput(sparkOutputFile);
        } else {
            sparkOutputFile = null;
        }
        File sparkErrorFile;
        if (StringUtils.isNotBlank(appErrorDir)) {
            sparkErrorFile = new File(appErrorDir, DRIVER_ERROR_FILENAME);
            sparkLauncher.redirectError(sparkErrorFile);
        } else {
            sparkErrorFile = null;
        }

        sparkLauncher.setAppResource(appResource)
                .setMaster(masterURI)
                .setSparkHome(sparkHomeDir)
                .setConf("spark.ui.showConsoleProgress", "false") // The console progress bar screws up the STDOUT stream
                .setConf(SparkLauncher.DRIVER_MEMORY, sparkDriverMemory)
                .setConf(SparkLauncher.EXECUTOR_MEMORY, sparkExecutorMemory)
                .setConf(SparkLauncher.EXECUTOR_CORES, "" + coresPerSparkExecutor)
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
            Stopwatch sparkAppWatch = Stopwatch.createStarted();

            SparkApp sparkApp = new SparkApp(this, sparkOutputFile, sparkErrorFile);

            SparkAppHandle.Listener completionListener = new SparkAppHandle.Listener() {
                @Override
                public void stateChanged(SparkAppHandle sparkAppHandle) {
                    SparkAppHandle.State sparkAppState = sparkAppHandle.getState();
                    logger.info("Spark application state changed: {}", sparkAppState);
                    if (sparkAppState.isFinal()) {
                        long seconds = sparkAppWatch.stop().elapsed().getSeconds();
                        logger.info("Spark application completed after {} seconds [{}]", seconds, sparkAppState);
                        if (sparkAppState != SparkAppHandle.State.FINISHED) {
                            logger.warn("Spark application {} finished with an error state {}", sparkApp.getAppId(), sparkAppState);
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

            return computationFactory.newCompletedComputation(sparkApp)
                    .thenSuspendUntil(app -> new ContinuationCond.Cond<>(app, app.isDone()),
                            appIntervalCheck,
                            appTimeout);
        } catch (Exception e) {
            logger.error("Error running application {} using {}", appResource, masterJobId, e);
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
        logger.info("Stopping Spark cluster {}", masterJobId);
        try {
            SparkApp sparkApp = updateCurrentApp(null);
            if (sparkApp != null && !sparkApp.isDone()) {
                logger.warn("Force killing Spark application {} running on cluster {}", sparkApp.getAppId(), masterJobId);
                sparkApp.kill();
            }
        } catch (Exception e) {
            logger.error("Error stopping Spark cluster {}", masterJobId, e);
        }
        try {
            logger.info("Kill master spark job {}", masterJobId);
            jobMgr.killJob(masterJobId);
        } catch (Exception e) {
            logger.error("Error stopping master spark job {}", masterJobId, e);
        }
        workerJobIds.forEach(jobId -> {
                    try {
                        logger.info("Kill spark worker job {} part of spark cluster {}", jobId, masterJobId);
                        jobMgr.killJob(jobId);
                    } catch (Exception e) {
                        logger.error("Error stopping spark job {} part of {} cluster", jobId, masterJobId, e);
                    }
                });
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("masterJobId", masterJobId)
                .append("workerJobIds", workerJobIds)
                .toString();
    }
}

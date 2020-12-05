package org.janelia.jacs2.asyncservice.spark;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.janelia.cluster.JobFuture;
import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobTemplate;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LSFSparkCluster {
    private static final Logger LOG = LoggerFactory.getLogger(LSFSparkCluster.class);
    private static final String DRIVER_ERROR_FILENAME = "sparkdriver.err";

    private final JobManager jobMgr;
    private final ServiceComputationFactory computationFactory;
    private final SparkClusterInfo sparkClusterInfo;
    private final String billingInfo;

    LSFSparkCluster(JobManager jobMgr,
                    ServiceComputationFactory computationFactory,
                    @Nonnull SparkClusterInfo sparkClusterInfo,
                    String billingInfo) {
        this.jobMgr = jobMgr;
        this.computationFactory = computationFactory;
        this.sparkClusterInfo = sparkClusterInfo;
        this.billingInfo = billingInfo;
    }

    SparkClusterInfo getSparkClusterInfo() {
        return sparkClusterInfo;
    }

    ServiceComputation<? extends SparkApp> runLocalProcessApp(String appResource,
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
        String[] appArgsArray;
        if (CollectionUtils.isEmpty(appArgs)) {
            appArgsArray = new String[0];
        } else {
            appArgsArray = appArgs.toArray(new String[0]);
        }
        sparkLauncher
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
        int sparkParallelism = SparkAppResourceHelper.getSparkParallelism(sparkAppResources);
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
            LocalProcessSparkApp sparkApp = new LocalProcessSparkApp(sparkAppHandle, sparkErrorFile != null ? sparkErrorFile.getAbsolutePath() : null);
            return computationFactory.newCompletedComputation(sparkApp)
                    .thenSuspendUntil(app -> new ContinuationCond.Cond<>(app, app.isDone()),
                            SparkAppResourceHelper.getSparkAppIntervalCheckInMillis(sparkAppResources),
                            SparkAppResourceHelper.getSparkAppTimeoutInMillis(sparkAppResources));
        } catch (Exception e) {
            LOG.error("Error running spark application {} on {}", appResource, sparkClusterInfo, e);
            return computationFactory.newFailedComputation(e);
        }
    }

    ServiceComputation<? extends SparkApp> runLSFApp(String appResource,
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
        File sparkErrorFile;
        if (StringUtils.isNotBlank(appErrorDir)) {
            sparkErrorFile = new File(appErrorDir, DRIVER_ERROR_FILENAME);
        } else {
            sparkErrorFile = null;
        }
        ImmutableList.Builder<String> driverOptionsBuilder = ImmutableList.builder();
        if (StringUtils.isNotBlank(appEntryPoint)) {
            driverOptionsBuilder.add("--class", appEntryPoint);
        }
        driverOptionsBuilder.add("--master", sparkClusterInfo.getMasterURI());
        driverOptionsBuilder.add(appResource);

        JobTemplate driverJobTemplate = createSparkDriverJobTemplate(
                "sparkdriver",
                driverOptionsBuilder.build(),
                SparkAppResourceHelper.getSparkHome(sparkAppResources),
                appOutputDir,
                appOutputDir,
                appErrorDir,
                createNativeSpec(SparkAppResourceHelper.getSparkDriverCores(sparkAppResources),
                        billingInfo,
                        SparkAppResourceHelper.getSparkAppTimeoutInMin(sparkAppResources)),
                Collections.emptyMap());
        // Submit driver job
        try {
            JobFuture driverJobFuture = jobMgr.submitJob(driverJobTemplate);
            LOG.info("Submitted spark job {} ", driverJobFuture.getJobId());
            LSFJobSparkApp sparkApp = new LSFJobSparkApp(jobMgr, driverJobFuture.getJobId(), sparkErrorFile != null ? sparkErrorFile.getAbsolutePath() : null);
            return computationFactory.newCompletedComputation(sparkApp)
                    .thenSuspendUntil(app -> new ContinuationCond.Cond<>(app, app.isDone()),
                            SparkAppResourceHelper.getSparkAppIntervalCheckInMillis(sparkAppResources),
                            SparkAppResourceHelper.getSparkAppTimeoutInMillis(sparkAppResources));
        } catch (Exception e) {
            LOG.error("Error running spark application {} on {}", appResource, sparkClusterInfo, e);
            return computationFactory.newFailedComputation(e);
        }
    }

    private JobTemplate createSparkDriverJobTemplate(String jobName,
                                                     List<String> jobOptions,
                                                     String sparkHomeDir,
                                                     String jobWorkingPath,
                                                     String jobOutputPath,
                                                     String jobErrorPath,
                                                     List<String> nativeSpec,
                                                     Map<String, String> jobEnv) {
        JobTemplate jt = new JobTemplate();
        jt.setJobName(jobName);
        jt.setWorkingDir(jobWorkingPath);
        jt.setOutputPath(jobOutputPath);
        jt.setErrorPath(jobErrorPath);
        jt.setRemoteCommand(sparkHomeDir + "/bin/spark-submit");
        jt.setArgs(jobOptions);
        jt.setNativeSpecification(nativeSpec);
        jt.setJobEnvironment(jobEnv);
        return jt;
    }

    private List<String> createNativeSpec(int nProcessingSlots, String billingAccount, int sparkClusterTimeoutInMins) {
        List<String> spec = new ArrayList<>();
        if (nProcessingSlots > 1) {
            spec.add("-n "+nProcessingSlots);
        }
        if (sparkClusterTimeoutInMins > 0) {
            spec.add("-W " + sparkClusterTimeoutInMins);
        }
        if (StringUtils.isNotBlank(billingAccount)) {
            spec.add("-P " + billingAccount);
        }
        return spec;
    }

    void stopCluster() {
        try {
            LOG.info("Kill spark master job {}", sparkClusterInfo.getMasterJobId());
            jobMgr.killJob(sparkClusterInfo.getMasterJobId());
        } catch (Exception e) {
            LOG.error("Error stopping spark master job {}", sparkClusterInfo.getMasterJobId(), e);
        }
        try {
            LOG.info("Kill spark worker job {}", sparkClusterInfo.getWorkerJobId());
            jobMgr.killJob(sparkClusterInfo.getWorkerJobId());
        } catch (Exception e) {
            LOG.error("Error stopping spark worker job {}", sparkClusterInfo.getWorkerJobId(), e);
        }
    }
}

package org.janelia.jacs2.asyncservice.spark;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.launcher.SparkLauncher;
import org.janelia.cluster.JobFuture;
import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LSFSparkDriverRunner implements SparkDriverRunner<LSFJobSparkApp> {
    private static final Logger LOG = LoggerFactory.getLogger(LSFSparkDriverRunner.class);

    private final JobManager jobMgr;
    private final String billingInfo;

    LSFSparkDriverRunner(JobManager jobMgr, String billingInfo) {
        this.jobMgr = jobMgr;
        this.billingInfo = billingInfo;
    }

    public LSFJobSparkApp startSparkApp(SparkClusterInfo sparkClusterInfo,
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
        String sparkDriverMemory = SparkAppResourceHelper.getSparkDriverMemory(sparkAppResources);
        if (StringUtils.isNotBlank(sparkDriverMemory)) {
            driverOptionsBuilder.add("--driver-memory").add(sparkDriverMemory);
        }
        int sparkWorkerCores = SparkAppResourceHelper.getSparkWorkerCores(sparkAppResources);
        driverOptionsBuilder.add("--conf").add(SparkLauncher.EXECUTOR_CORES + "=" + sparkWorkerCores);
        int sparkMemPerCoreInGB = SparkAppResourceHelper.getSparkWorkerMemoryPerCoreInGB(sparkAppResources);
        if (sparkMemPerCoreInGB > 0) {
            driverOptionsBuilder.add("--executor-memory").add(sparkWorkerCores * sparkMemPerCoreInGB + "g");
        }
        int appDefinedParallelism = SparkAppResourceHelper.getSparkParallelism(sparkAppResources);
        int defaultSparkParallelism = 3 * sparkWorkerCores * SparkAppResourceHelper.getSparkWorkers(sparkAppResources);
        int sparkParallelism = appDefinedParallelism != 0 ? appDefinedParallelism : defaultSparkParallelism;
        if (sparkParallelism > 0) {
            // The default (4MB) open cost consolidates files into tiny partitions regardless of number of cores.
            // By forcing this parameter to zero, we can specify the exact parallelism we want.
            driverOptionsBuilder.add("--conf").add("spark.files.openCostInBytes=0");
            driverOptionsBuilder.add("--conf").add("spark.default.parallelism=" + sparkParallelism);
        }
        StringBuilder sparkDriverJavaOptsBuilder = new StringBuilder();
        String sparkLogConfigFile = SparkAppResourceHelper.getSparkLogConfigFile(sparkAppResources);
        if (StringUtils.isNotBlank(sparkLogConfigFile)) {
            driverOptionsBuilder.add("--conf").add(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS+"=-Dlog4j.configuration=file://" + sparkLogConfigFile);
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
            driverOptionsBuilder.add("--driver-java-options", sparkDriverJavaOptsBuilder.toString());
        }
        driverOptionsBuilder.add(appResource);
        if (CollectionUtils.isNotEmpty(appArgs)) {
            driverOptionsBuilder.addAll(appArgs);
        }

        JobTemplate driverJobTemplate = createSparkDriverJobTemplate(
                "sparkdriver",
                driverOptionsBuilder.build(),
                SparkAppResourceHelper.getSparkHome(sparkAppResources),
                appOutputDir,
                Paths.get(appOutputDir, DRIVER_OUTPUT_FILENAME).toString(),
                Paths.get(appErrorDir, DRIVER_ERROR_FILENAME).toString(),
                createNativeSpec(SparkAppResourceHelper.getSparkDriverCores(sparkAppResources),
                        billingInfo,
                        SparkAppResourceHelper.getSparkAppTimeoutInMin(sparkAppResources)),
                Collections.emptyMap());
        // Submit driver job
        try {
            JobFuture driverJobFuture = jobMgr.submitJob(driverJobTemplate);
            LOG.info("Submitted spark job {} ", driverJobFuture.getJobId());
            return new LSFJobSparkApp(jobMgr, driverJobFuture.getJobId(), sparkErrorFile != null ? sparkErrorFile.getAbsolutePath() : null);
        } catch (Exception e) {
            LOG.error("Error running spark application {} on {}", appResource, sparkClusterInfo, e);
            throw new IllegalStateException(e);
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
            spec.add("-n " + nProcessingSlots);
        }
        if (sparkClusterTimeoutInMins > 0) {
            spec.add("-W " + sparkClusterTimeoutInMins);
        }
        if (StringUtils.isNotBlank(billingAccount)) {
            spec.add("-P " + billingAccount);
        }
        return spec;
    }

}

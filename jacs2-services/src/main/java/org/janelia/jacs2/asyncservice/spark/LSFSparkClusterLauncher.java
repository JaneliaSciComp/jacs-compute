package org.janelia.jacs2.asyncservice.spark;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.cluster.JobFuture;
import org.janelia.cluster.JobInfo;
import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobStatus;
import org.janelia.cluster.JobTemplate;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.cluster.LsfParseUtils;
import org.janelia.jacs2.asyncservice.common.cluster.MonitoredJobManager;
import org.janelia.jacs2.cdi.qualifier.BoolPropertyValue;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.model.service.JacsJobInstanceInfo;
import org.slf4j.Logger;

/**
 * Launch a Spark cluster on LSF.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class LSFSparkClusterLauncher {

    private final static String DEFAULT_SPARK_URI_SCHEME = "spark";
    private final static int DEFAULT_SPARK_MASTER_PORT = 7077;

    private static final ExecutorService COMPLETION_MESSAGE_EXECUTOR = Executors.newCachedThreadPool((runnable) -> {
        Thread thread = Executors.defaultThreadFactory().newThread(runnable);
        thread.setName("SparkCompletionMessageThread");
        // Ensure that we can shut down without these threads getting in the way
        thread.setDaemon(true);
        return thread;
    });

    // Configuration
    private final Logger logger;
    private final ServiceComputationFactory computationFactory;
    private final JobManager jobMgr;
    private final boolean requiresAccountInfo;
    private final long clusterStartTimeoutInMillis;
    private final long clusterIntervalCheckInMillis;
    private final String sparkLSFSpec;
    private final String startSparkCmd;
    private final String sparkMasterClass;
    private final String sparkWorkerClass;

    @Inject
    public LSFSparkClusterLauncher(ServiceComputationFactory computationFactory,
                                   MonitoredJobManager monitoredJobManager,
                                   @BoolPropertyValue(name = "service.cluster.requiresAccountInfo", defaultValue = true) boolean requiresAccountInfo,
                                   @IntPropertyValue(name = "service.spark.cluster.startTimeoutInSeconds", defaultValue = 3600) int clusterStartTimeoutInSeconds,
                                   @IntPropertyValue(name = "service.spark.cluster.intervalCheckInMillis", defaultValue = 2000) int clusterIntervalCheckInMillis,
                                   @StrPropertyValue(name = "service.spark.startCommand", defaultValue = "spark-class") String startSparkCmd,
                                   @StrPropertyValue(name = "service.spark.sparkMasterClass", defaultValue = "org.apache.spark.deploy.master.Master") String sparkMasterClass,
                                   @StrPropertyValue(name = "service.spark.sparkWorkerClass", defaultValue = "org.apache.spark.deploy.worker.Worker") String sparkWorkerClass,
                                   @StrPropertyValue(name = "service.spark.lsf.spec", defaultValue = "") String sparkLSFSpec,
                                   Logger logger) {
        this.computationFactory = computationFactory;
        this.jobMgr = monitoredJobManager.getJobMgr();
        this.requiresAccountInfo = requiresAccountInfo;
        this.clusterStartTimeoutInMillis = clusterStartTimeoutInSeconds > 0 ? clusterStartTimeoutInSeconds * 1000 : -1;
        this.clusterIntervalCheckInMillis = clusterIntervalCheckInMillis;
        this.sparkLSFSpec = sparkLSFSpec;
        this.startSparkCmd = startSparkCmd;
        this.sparkMasterClass = sparkMasterClass;
        this.sparkWorkerClass = sparkWorkerClass;
        this.logger = logger;
    }

    public ServiceComputation<LSFSparkCluster> startCluster(String sparkJobName,
                                                            String sparkHomeDir,
                                                            int nWorkers,
                                                            int nCoresPerWorker,
                                                            int minRequiredWorkersParam,
                                                            Path jobWorkingPath,
                                                            Path jobOutputPath,
                                                            Path jobErrorPath,
                                                            String billingInfo,
                                                            int sparkJobsTimeoutInMins) {
        int minRequiredWorkers;
        if (minRequiredWorkersParam < 0 || minRequiredWorkersParam > nWorkers) {
            minRequiredWorkers = nWorkers;
        } else {
            minRequiredWorkers = minRequiredWorkersParam;
        }
        String sparkConfigFile = createSparkConfigFile(jobWorkingPath.resolve("spark-config.properties").toFile());
        // Start the master first
        return startSparkMasterJob(
                sparkJobName,
                sparkHomeDir,
                jobWorkingPath,
                jobOutputPath,
                jobErrorPath,
                billingInfo,
                sparkConfigFile,
                sparkJobsTimeoutInMins)

                // Wait for the master job to start
                .thenSuspendUntil(
                        masterJobId -> {
                            logger.trace("Check if spark cluster {} is ready", masterJobId);
                            Collection<JobInfo> jobInfos = jobMgr.getJobInfo(masterJobId);
                            return jobInfos.stream().findFirst().orElse(null);
                        },
                        (JobInfo masterJobInfo) -> {
                            if (masterJobInfo != null) {
                                if (masterJobInfo.isComplete()) {
                                    logger.error("Spark master job {} has already completed so nothing can be submitted to the cluster", masterJobInfo);
                                    throw new IllegalStateException("Spark master job " + masterJobInfo.getJobId() + " has already completed before starting the application");
                                }
                                if (masterJobInfo.getStatus() == JobStatus.RUNNING) {
                                    logger.info("Master job {} is running", masterJobInfo.getJobId());
                                    return new ContinuationCond.Cond<>(masterJobInfo, true);
                                } else {
                                    return new ContinuationCond.Cond<>(masterJobInfo, false);
                                }
                            } else {
                                return new ContinuationCond.Cond<>(null, false);
                            }
                        },
                        clusterIntervalCheckInMillis,
                        clusterStartTimeoutInMillis)

                // Now wait until the master is actually ready to accept connections.
                // Without this step, the workers and driver may connect too early and we'll get messages like this:
                // "Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources"
                // These types of warnings normally wouldn't matter much, except that Janelia's start worker script waits
                // 2 minutes to retry if the worker fails to start because it can't connect to the master

                // Now we're ready to spawn the workers and have them connect back to the master
                .thenCompose(masterJobInfo -> {
                    String sparkMasterURI = getSparkURIFromJobInfo(masterJobInfo);
                    logger.info("Spark master job {} ({}) is running on {}", masterJobInfo.getJobId(), sparkMasterURI, masterJobInfo.getExecHost());
                    return startSparkWorkerJobs(
                            sparkJobName,
                            sparkHomeDir,
                            jobWorkingPath,
                            jobOutputPath,
                            jobErrorPath,
                            billingInfo,
                            sparkConfigFile,
                            sparkJobsTimeoutInMins,
                            nWorkers,
                            nCoresPerWorker,
                            sparkMasterURI)

                            // Wait for the minimum number of workers to start
                            .thenSuspendUntil(
                                    workerJobId -> {
                                        logger.trace("Check if spark workers job {} is ready", workerJobId);
                                        return Pair.of(workerJobId, jobMgr.getJobInfo(workerJobId));
                                    },
                                    (Pair<Long, Collection<JobInfo>> workersInfo) -> {
                                        long runningWorkers = workersInfo.getRight().stream()
                                                .filter(ji -> ji.getStatus() == JobStatus.RUNNING)
                                                .count();
                                        if (runningWorkers >= minRequiredWorkers) {
                                            logger.info("Reached the minimum workers requirement {} for {} - {} workers are running", minRequiredWorkers, workersInfo.getLeft(), runningWorkers);
                                            return new ContinuationCond.Cond<>(workersInfo, true);
                                        } else {
                                            return new ContinuationCond.Cond<>(workersInfo, false);
                                        }
                                    },
                                    clusterIntervalCheckInMillis,
                                    clusterStartTimeoutInMillis)

                            // then create the spark cluster
                            .thenApply(workersInfo -> createSparkCluster(new SparkClusterInfo(masterJobInfo.getJobId(), workersInfo.getLeft(), sparkMasterURI)));
                });
    }

    private String createSparkConfigFile(File sparkConfigFile) {
        // for now just create a file with some defaults in case spark-defaults.conf is missing
        Properties sparkConfig = new Properties();
        sparkConfig.put("spark.rpc.askTimeout", "300s");
        sparkConfig.put("spark.storage.blockManagerHeartBeatMs", "30000");
        sparkConfig.put("spark.rpc.retry.wait", "30s");
        sparkConfig.put("spark.kryoserializer.buffer.max", "1024m");
        sparkConfig.put("spark.core.connection.ack.wait.timeout", "600s");
        try (FileOutputStream sparkConfigStream = new FileOutputStream(sparkConfigFile)) {
            sparkConfig.store(sparkConfigStream, null);
        } catch (Exception e) {
            logger.warn("Error writing spark config to {}", sparkConfigFile, e);
            return null;
        }
        return sparkConfigFile.getAbsolutePath();
    }

    LSFSparkCluster createSparkCluster(Long masterJobId, Long workerJobId, String billingInfo) {
        return createSparkCluster(new SparkClusterInfo(masterJobId, workerJobId, getSparkURIFromJobId(masterJobId)));
    }

    SparkDriverRunner<? extends SparkApp> getLocalDriverRunner() {
        return new LocalSparkDriverRunner();
    }

    SparkDriverRunner<? extends SparkApp> getLSFDriverRunner(String billingInfo) {
        return new LSFSparkDriverRunner(jobMgr, billingInfo);
    }

    private LSFSparkCluster createSparkCluster(SparkClusterInfo sparkClusterInfo) {
        return new LSFSparkCluster(jobMgr, sparkClusterInfo);
    }

    private ServiceComputation<Long> startSparkMasterJob(String jobName,
                                                         String sparkHomeDir,
                                                         Path jobWorkingPath,
                                                         Path jobOutputPath,
                                                         Path jobErrorPath,
                                                         String billingInfo,
                                                         String sparkConfigFile,
                                                         int sparkJobsTimeoutInMins) {
        logger.info("Starting spark {} master job {} with working directory {}", sparkHomeDir, jobName, jobWorkingPath);
        logger.info("Spark master output dir: {}", jobOutputPath);
        logger.info("Spark master error dir: {}", jobErrorPath);
        try {
            ImmutableList.Builder<String> jobArgsBuilder = ImmutableList.<String>builder().add(sparkMasterClass);
            if (StringUtils.isNotBlank(sparkConfigFile)) {
                jobArgsBuilder.add("--properties-file").add(sparkConfigFile);
            }
            JobTemplate masterJobTemplate = createSparkJobTemplate(
                    "M" + jobName,
                    jobArgsBuilder.build(),
                    sparkHomeDir,
                    jobWorkingPath,
                    jobOutputPath.resolve("M" + jobName + ".out"),
                    jobErrorPath.resolve("M" + jobName + ".err"),
                    createNativeSpec(1, billingInfo, sparkJobsTimeoutInMins),
                    Collections.emptyMap()
            );
            // Submit master job
            JobFuture masterJobFuture = jobMgr.submitJob(masterJobTemplate);
            logger.info("Submitted master spark job {} ", masterJobFuture.getJobId());
            masterJobFuture.whenCompleteAsync((jobInfos, exc) -> logJobInfo(jobInfos), COMPLETION_MESSAGE_EXECUTOR);
            return computationFactory.newCompletedComputation(masterJobFuture.getJobId());
        } catch (Exception e) {
            logger.error("Error starting spark master node for {}", jobWorkingPath, e);
            return computationFactory.newFailedComputation(e);
        }
    }

    private ServiceComputation<Long> startSparkWorkerJobs(String jobName,
                                                          String sparkHomeDir,
                                                          Path jobWorkingPath,
                                                          Path jobOutputPath,
                                                          Path jobErrorPath,
                                                          String billingInfo,
                                                          String sparkConfigFile,
                                                          int sparkJobsTimeoutInMins,
                                                          int nWorkers,
                                                          int nCoresPerWorker,
                                                          String masterURI) {
        logger.info("Starting {} Spark {} workers with master {} and working directory {}", nWorkers, sparkHomeDir, masterURI, jobWorkingPath);
        try {
            ImmutableList.Builder<String> jobArgsBuilder = ImmutableList.<String>builder().add(sparkWorkerClass);
            if (nCoresPerWorker > 1) {
                jobArgsBuilder.add("-c").add(String.valueOf(nCoresPerWorker));
            }
            jobArgsBuilder.add("-d").add(jobWorkingPath.toString());
            if (StringUtils.isNotBlank(sparkConfigFile)) {
                jobArgsBuilder.add("--properties-file").add(sparkConfigFile);
            }
            jobArgsBuilder.add(masterURI);
            JobTemplate workerJobTemplate = createSparkJobTemplate(
                    "W" + jobName,
                    jobArgsBuilder.build(),
                    sparkHomeDir,
                    jobWorkingPath,
                    jobOutputPath.resolve("W" + jobName  + "_#.out"),
                    jobErrorPath.resolve("W" + jobName + "_#.err"),
                    createNativeSpec(nCoresPerWorker, billingInfo, sparkJobsTimeoutInMins),
                    Collections.emptyMap()
            );
            JobFuture workerJobsFuture = jobMgr.submitJob(workerJobTemplate, 1, nWorkers);
            logger.info("Submitted {} spark worker jobs {} ", nWorkers, workerJobsFuture.getJobId());
            workerJobsFuture.whenCompleteAsync((jobInfos, exc) -> logJobInfo(jobInfos), COMPLETION_MESSAGE_EXECUTOR);
            return computationFactory.newCompletedComputation(workerJobsFuture.getJobId());
        } catch (Exception e) {
            logger.error("Error starting spark workers for {}", jobWorkingPath, e);
            return computationFactory.newFailedComputation(e);
        }
    }

    private JobTemplate createSparkJobTemplate(String jobName,
                                               List<String> jobOptions,
                                               String sparkHomeDir,
                                               Path jobWorkingPath,
                                               Path jobOutputPath,
                                               Path jobErrorPath,
                                               List<String> nativeSpec,
                                               Map<String, String> jobEnv) {
        JobTemplate jt = new JobTemplate();
        jt.setJobName(jobName);
        jt.setWorkingDir(jobWorkingPath.toString());
        jt.setOutputPath(jobOutputPath.toString());
        jt.setErrorPath(jobErrorPath.toString());
        jt.setRemoteCommand(sparkHomeDir + "/bin/" + startSparkCmd);
        jt.setArgs(jobOptions);
        jt.setNativeSpecification(nativeSpec);
        jt.setJobEnvironment(jobEnv);
        return jt;
    }

    private List<String> createNativeSpec(int nProcessingSlots, String billingAccount, int sparkClusterTimeoutInMins) {
        List<String> spec = new ArrayList<>();
        if (nProcessingSlots > 1) {
            // append processing environment
            spec.add("-n " + nProcessingSlots);
        }
        if (StringUtils.isNotBlank(sparkLSFSpec)) {
            spec.addAll(Splitter.on(' ').trimResults().omitEmptyStrings().splitToList(sparkLSFSpec));
        }
        if (sparkClusterTimeoutInMins > 0) {
            spec.add("-W " + sparkClusterTimeoutInMins);
        }
        if (requiresAccountInfo)
            spec.add("-P " + billingAccount);

        return spec;
    }

    /**
     * Get SparkURI given the master job ID.
     *
     * @param jobId - master job ID
     * @return
     */
    private String getSparkURIFromJobId(Long jobId) {
        Collection<JobInfo> jobInfos = jobMgr.getJobInfo(jobId);
        return jobInfos.stream()
                .map(jobInfo -> getSparkURIFromJobInfo(jobInfo))
                .findFirst()
                .orElse(null);
    }

    private String getSparkURIFromJobInfo(JobInfo jobInfo) {
        return getHostname(jobInfo)
                .map(execHost -> DEFAULT_SPARK_URI_SCHEME + "://" + execHost + ":" + DEFAULT_SPARK_MASTER_PORT)
                .orElseThrow(() -> {
                    logger.error("No proper exec hosts has been set for {} even though exec host field is set to {}", jobInfo, jobInfo.getExecHost());
                    return new IllegalStateException("No proper exec hosts has been set for " + jobInfo + " even though exec host field is set to " + jobInfo.getExecHost());
                });
    }

    private Optional<String> getHostname(JobInfo jobInfo) {
        String jobExecHost = jobInfo.getExecHost();
        if (StringUtils.isBlank(jobExecHost)) {
            throw new IllegalStateException("No exec host found for " + jobInfo);
        } else {
            return Arrays.stream(jobExecHost.split(":"))
                    .filter(StringUtils::isNotBlank)
                    .map(execHost -> {
                        int coreSeparatorIndex = execHost.indexOf('*');
                        if (coreSeparatorIndex == -1) {
                            return execHost;
                        } else {
                            return execHost.substring(coreSeparatorIndex + 1);
                        }
                    })
                    .filter(StringUtils::isNotBlank)
                    .findFirst();
        }
    }

    private void logJobInfo(Collection<JobInfo> jobInfos) {
        jobInfos.stream()
                .map(this::toJobInstanceInfo)
                .forEach(jobInstanceInfo -> {
                    Long queueTimeSeconds = jobInstanceInfo.getQueueSecs();
                    Long runTimeSeconds = jobInstanceInfo.getRunSecs();

                    String queueTime = formatTime(queueTimeSeconds);
                    String runTime = formatTime(runTimeSeconds);

                    String maxMem = jobInstanceInfo.getMaxMem();
                    String jobIdStr = jobInstanceInfo.getJobId() + "";
                    if (jobInstanceInfo.getArrayIndex() != null) {
                        jobIdStr += "." + jobInstanceInfo.getArrayIndex();
                    }
                    logger.info("Spark cluster job {} was queued for {}, ran for {}, and used {} of memory.", jobIdStr, queueTime, runTime, maxMem);
                    if ("TERM_OWNER".equals(jobInstanceInfo.getExitReason())) { // We expect the cluster to be closed by us, so TERM_OWNER is not an error
                        logger.error("Spark cluster job {} exited with code {} and reason {}", jobIdStr, jobInstanceInfo.getExitCode(), jobInstanceInfo.getExitReason());
                    }
                });
    }

    private String formatTime(Long timeInSeconds) {
        if (timeInSeconds == null) {
            return "<unknown>";
        } else if (timeInSeconds > 300) { // more than 5 minutes, just show the minutes
            return TimeUnit.MINUTES.convert(timeInSeconds, TimeUnit.SECONDS) + " min";
        } else {
            return timeInSeconds + " sec";
        }
    }

    private JacsJobInstanceInfo toJobInstanceInfo(JobInfo jobInfo) {
        JacsJobInstanceInfo jobInstanceInfo = new JacsJobInstanceInfo();
        jobInstanceInfo.setJobId(jobInfo.getJobId());
        jobInstanceInfo.setArrayIndex(jobInfo.getArrayIndex());
        jobInstanceInfo.setName(jobInfo.getName());
        jobInstanceInfo.setFromHost(jobInfo.getFromHost());
        jobInstanceInfo.setExecHost(jobInfo.getExecHost());
        jobInstanceInfo.setStatus(jobInfo.getStatus() == null ? null : jobInfo.getStatus().name());
        jobInstanceInfo.setQueue(jobInfo.getQueue());
        jobInstanceInfo.setProject(jobInfo.getProject());
        jobInstanceInfo.setReqSlot(jobInfo.getReqSlot());
        jobInstanceInfo.setAllocSlot(jobInfo.getAllocSlot());
        jobInstanceInfo.setSubmitTime(LsfParseUtils.convertLocalDateTime(jobInfo.getSubmitTime()));
        jobInstanceInfo.setStartTime(LsfParseUtils.convertLocalDateTime(jobInfo.getStartTime()));
        jobInstanceInfo.setFinishTime(LsfParseUtils.convertLocalDateTime(jobInfo.getFinishTime()));
        jobInstanceInfo.setQueueSecs(LsfParseUtils.getDiffSecs(jobInfo.getSubmitTime(), jobInfo.getStartTime()));
        jobInstanceInfo.setRunSecs(LsfParseUtils.getDiffSecs(jobInfo.getStartTime(), jobInfo.getFinishTime()));
        jobInstanceInfo.setMaxMem(jobInfo.getMaxMem());
        jobInstanceInfo.setMaxMemBytes(LsfParseUtils.parseMemToBytes(jobInfo.getMaxMem()));
        jobInstanceInfo.setExitCode(jobInfo.getExitCode());
        jobInstanceInfo.setExitReason(jobInfo.getExitReason());
        return jobInstanceInfo;
    }
}

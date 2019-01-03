package org.janelia.jacs2.asyncservice.spark;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.cluster.JobFuture;
import org.janelia.cluster.JobInfo;
import org.janelia.cluster.JobInfoBuilder;
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

import javax.inject.Inject;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Start an LSF Spark cluster.
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
    private final int nodeSlots;
    private final String sparkVersion;
    private final String sparkHomeDir;
    private final String defaultSparkDriverMemory;
    private final String defaultSparkExecutorMemory;
    private final int defaultCoresPerSparkExecutor; // default number of cores per spark executor
    private final int sparkClusterHardDurationMins;
    private final String defaultSparkLogConfigFile;
    private final long clusterStartTimeoutInMillis;
    private final long clusterIntervalCheckInMillis;
    private final String hadoopHomeDir;
    private final String lsfApplication;
    private final String lsfRemoteCommand;

    @Inject
    public LSFSparkClusterLauncher(ServiceComputationFactory computationFactory,
                                   MonitoredJobManager monitoredJobManager,
                                   @BoolPropertyValue(name = "service.cluster.requiresAccountInfo", defaultValue = true) boolean requiresAccountInfo,
                                   @IntPropertyValue(name = "service.spark.nodeSlots", defaultValue = 32) int nodeSlots,
                                   @IntPropertyValue(name = "service.spark.workerCores", defaultValue = 30) int sparkWorkerCores,
                                   @StrPropertyValue(name = "service.spark.sparkVersion", defaultValue = "2.3.1") String sparkVersion,
                                   @StrPropertyValue(name = "service.spark.sparkHomeDir", defaultValue = "/misc/local/spark-2.3.1") String sparkHomeDir,
                                   @StrPropertyValue(name = "service.spark.driver.memory", defaultValue = "1g") String defaultSparkDriverMemory,
                                   @StrPropertyValue(name = "service.spark.executor.memory", defaultValue = "75g") String defaultSparkExecutorMemory,
                                   @IntPropertyValue(name = "service.spark.executor.cores", defaultValue = 5) int defaultCoresPerSparkExecutor,
                                   @IntPropertyValue(name = "service.spark.cluster.hard.duration.mins", defaultValue = 30) int sparkClusterHardDurationMins,
                                   @StrPropertyValue(name = "service.spark.log4jconfig.filepath", defaultValue = "") String defaultSparkLogConfigFile,
                                   @IntPropertyValue(name = "service.spark.cluster.startTimeoutInSeconds", defaultValue = 3600) int clusterStartTimeoutInSeconds,
                                   @IntPropertyValue(name = "service.spark.cluster.intervalCheckInMillis", defaultValue = 2000) int clusterIntervalCheckInMillis,
                                   @StrPropertyValue(name = "hadoop.homeDir") String hadoopHomeDir,
                                   @StrPropertyValue(name = "service.spark.lsf.application", defaultValue="spark32") String lsfApplication,
                                   @StrPropertyValue(name = "service.spark.lsf.remoteCommand", defaultValue="commandstring") String lsfRemoteCommand,
                                   Logger logger) {
        this.computationFactory = computationFactory;
        this.jobMgr = monitoredJobManager.getJobMgr();
        this.requiresAccountInfo = requiresAccountInfo;
        this.nodeSlots = nodeSlots;
        this.sparkVersion = sparkVersion;
        this.sparkHomeDir = sparkHomeDir;
        this.defaultSparkDriverMemory = defaultSparkDriverMemory;
        this.defaultSparkExecutorMemory = defaultSparkExecutorMemory;
        this.defaultCoresPerSparkExecutor = defaultCoresPerSparkExecutor;
        this.sparkClusterHardDurationMins = sparkClusterHardDurationMins;
        this.defaultSparkLogConfigFile = defaultSparkLogConfigFile;
        this.clusterStartTimeoutInMillis = clusterStartTimeoutInSeconds * 1000;
        this.clusterIntervalCheckInMillis = clusterIntervalCheckInMillis;
        this.hadoopHomeDir = hadoopHomeDir;
        this.lsfApplication = lsfApplication;
        this.lsfRemoteCommand = lsfRemoteCommand;
        this.logger = logger;
    }

    public ServiceComputation<SparkCluster> startCluster(int numNodes,
                                                         int minRequiredWorkersParam,
                                                         Path jobWorkingPath,
                                                         String billingInfo,
                                                         String sparkDriverMemory,
                                                         String sparkExecutorMemory,
                                                         String sparkLogConfigFile) {
        int minRequiredWorkers;
        if (minRequiredWorkersParam <= 0) {
            minRequiredWorkers = 0;
        } else if (minRequiredWorkersParam > numNodes) {
            minRequiredWorkers = numNodes;
        } else {
            minRequiredWorkers = minRequiredWorkersParam;
        }
        return startSparkMasterJob(jobWorkingPath, billingInfo)
                .thenCompose(masterJobInfo -> startSparkWorkerJobs(masterJobInfo, numNodes, minRequiredWorkers, jobWorkingPath, billingInfo, sparkDriverMemory, sparkExecutorMemory, sparkLogConfigFile))
                .thenCompose(sparkCluster -> waitForSparkCluster(sparkCluster))
                ;
    }

    private ServiceComputation<JobInfo> startSparkMasterJob(Path jobWorkingPath, String billingInfo) {
        logger.info("Starting spark {} master job with working directory", sparkVersion, jobWorkingPath);
        try {
            JobTemplate masterJobTemplate = createSparkJobTemplate(jobWorkingPath.toString(), createNativeSpec(null, billingInfo, "master"));
            // Submit master job
            JobFuture masterJobFuture = jobMgr.submitJob(masterJobTemplate);
            logger.info("Submitted master spark job {} ", masterJobFuture.getJobId());
            return computationFactory.newCompletedComputation(masterJobFuture.getJobId())
                    .thenApply(jobId -> {
                        Collection<JobInfo> jobInfos = jobMgr.retrieveJobInfo(jobId);
                        if (CollectionUtils.size(jobInfos) > 1) {
                            logger.warn("More than one job found for master Spark job {} - {} ", jobId, jobInfos);
                        }
                        return jobInfos.stream().findFirst().orElseThrow(() -> new IllegalStateException(new IllegalStateException("Error starting master spark node for " + jobWorkingPath)));
                    });
        } catch(Exception e) {
            logger.error("Error starting spark master node for {}", jobWorkingPath, e);
            return computationFactory.newFailedComputation(e);
        }
    }

    private ServiceComputation<SparkCluster> startSparkWorkerJobs(JobInfo masterJobInfo,
                                                                  int numNodes,
                                                                  int minRequiredWorkers,
                                                                  Path jobWorkingPath,
                                                                  String billingInfo,
                                                                  String sparkDriverMemory,
                                                                  String sparkExecutorMemory,
                                                                  String sparkLogConfigFile) {
        logger.info("Starting Spark-{} cluster with master {} + {} worker nodes and working directory {}", sparkVersion, masterJobInfo, numNodes, jobWorkingPath);

        List<JobFuture> workerJobs = IntStream.range(0, numNodes)
                .mapToObj(ni -> createSparkJobTemplate(jobWorkingPath.toString(), createNativeSpec(masterJobInfo.getJobId(), billingInfo, "worker")))
                .map(jt -> {
                    try {
                        JobFuture workerJob = jobMgr.submitJob(jt);
                        logger.info("Submitted worker spark job {} part of {} cluster", workerJob.getJobId(), masterJobInfo.getJobId());
                        return workerJob;
                    } catch(Exception e) {
                        logger.error("Error starting one spark worker out of {} for cluster {}", numNodes, masterJobInfo, e);
                        return JobFuture.withException(e);
                    }
                })
                .collect(Collectors.toList());
        return computationFactory.newCompletedComputation(new SparkCluster(
                computationFactory,
                jobMgr,
                masterJobInfo.getJobId(),
                workerJobs.stream().filter(jf -> !jf.isCompletedExceptionally()).map(jf -> jf.getJobId()).collect(Collectors.toList()),
                minRequiredWorkers,
                null, // no master URI yet because the master job may still be waiting
                sparkHomeDir,
                hadoopHomeDir,
                StringUtils.defaultIfBlank(sparkDriverMemory, defaultSparkDriverMemory),
                StringUtils.defaultIfBlank(sparkExecutorMemory, defaultSparkExecutorMemory),
                defaultCoresPerSparkExecutor,
                calculateDefaultParallelism(numNodes),
                StringUtils.defaultIfBlank(sparkLogConfigFile, defaultSparkLogConfigFile),
                logger)
        );
    }

    private JobTemplate createSparkJobTemplate(String jobWorkingPath, List<String> nativeSpec) {
        JobTemplate jt = new JobTemplate();
        jt.setJobName("sparkjacs");
        jt.setArgs(Collections.emptyList());
        jt.setWorkingDir(jobWorkingPath);
        jt.setRemoteCommand(lsfRemoteCommand);
        jt.setNativeSpecification(nativeSpec);
        return jt;
    }

    int calculateDefaultParallelism(int numNodes) {
        // Default to three tasks per slot (this seems empirically optimal)
        return 3 * (nodeSlots / defaultCoresPerSparkExecutor) * numNodes;
    }

    ServiceComputation<SparkCluster> createCluster(Long masterJobId,
                                                   List<Long> workerJobIds,
                                                   int minRequiredWorkers,
                                                   int defaultParallelism,
                                                   String sparkDriverMemory,
                                                   String sparkExecutorMemory,
                                                   String sparkLogConfigFile) {
        return computationFactory.newCompletedComputation(new SparkCluster(
                computationFactory,
                jobMgr,
                masterJobId,
                workerJobIds,
                minRequiredWorkers,
                null, // no master URI yet because the master job may still be waiting
                sparkHomeDir,
                hadoopHomeDir,
                StringUtils.defaultIfBlank(sparkDriverMemory, defaultSparkDriverMemory),
                StringUtils.defaultIfBlank(sparkExecutorMemory, defaultSparkExecutorMemory),
                defaultCoresPerSparkExecutor,
                defaultParallelism,
                StringUtils.defaultIfBlank(sparkLogConfigFile, defaultSparkLogConfigFile),
                logger))
                .thenCompose(sparkCluster -> waitForSparkCluster(sparkCluster))
                ;
    }

    private ServiceComputation<SparkCluster> waitForSparkCluster(SparkCluster sparkCluster) {
        return computationFactory.newCompletedComputation(sparkCluster)
                .thenSuspendUntil( // wait for the master job info to start
                        (SparkCluster aSparkCluster) -> {
                            logger.trace("Check if spark cluster {} is ready", aSparkCluster);
                            Collection<JobInfo> jobInfos = jobMgr.getJobInfo(sparkCluster.getMasterJobId());
                            return jobInfos.stream().findFirst().orElseThrow(() -> new IllegalStateException(new IllegalStateException("Error checking spark master node for " + sparkCluster)));
                        },
                        (JobInfo masterJobInfo) -> {
                            if (masterJobInfo != null) {
                                if (masterJobInfo.isComplete()) {
                                    logger.error("Spark master job {} has already completed so nothing can be submitted to the cluster", masterJobInfo);
                                    throw new IllegalStateException("Spark master job " + masterJobInfo.getJobId() + " has already completed before starting the application");
                                }
                                if (masterJobInfo.getStatus() == JobStatus.RUNNING) {
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
                .thenApply(masterJobInfo -> {
                    String sparkMasterURI = getSparkURIFromJobInfo(masterJobInfo);
                    logger.info("Spark master job {} ({}) is running on {}", masterJobInfo.getJobId(), sparkMasterURI, masterJobInfo.getExecHost());
                    return new SparkCluster(
                            computationFactory,
                            jobMgr,
                            masterJobInfo.getJobId(),
                            sparkCluster.getWorkerJobIds(),
                            sparkCluster.getMinRequiredWorkers(),
                            sparkMasterURI,
                            sparkHomeDir,
                            hadoopHomeDir,
                            sparkCluster.getSparkDriverMemory(),
                            sparkCluster.getSparkExecutorMemory(),
                            defaultCoresPerSparkExecutor,
                            sparkCluster.getDefaultParallelism(),
                            sparkCluster.getSparkLogConfigFile(),
                            logger);
                })
                // wait for a minimum number of workers to be running
                .thenSuspendUntil(updatedSparkCluster -> new ContinuationCond.Cond<>(updatedSparkCluster, checkMininimumWorkerRequirement(updatedSparkCluster)),
                        clusterIntervalCheckInMillis,
                        clusterStartTimeoutInMillis)
                .exceptionally(exc -> {
                    try {
                        // this may happen if starting the cluster timed out
                        logger.error("Killing spark cluster {} because of an exception", sparkCluster, exc);
                        sparkCluster.stopCluster();
                    } catch (Exception ignore) {
                        logger.warn("Exception trying to kill {}", sparkCluster, ignore);
                    }
                    throw new IllegalStateException(exc);
                })
                ;
    }

    private List<String> createNativeSpec(Long masterJobId, String billingAccount, String nodeType) {
        List<String> spec = new ArrayList<>();
        spec.add("-a " + String.format("%s(%s,%s)", lsfApplication, nodeType, sparkVersion)); // spark32(master,2.3.1) or spark32(worker,2.31.
        spec.add("-W "+ sparkClusterHardDurationMins);
        if (masterJobId != null && masterJobId > 0L) {
            spec.add("-J W" + masterJobId);
        }
        if (requiresAccountInfo)
            spec.add("-P "+billingAccount);

        return spec;
    }

    private String getSparkURIFromJobInfo(JobInfo jobInfo) {
        String jobExecHost = jobInfo.getExecHost();
        if (StringUtils.isBlank(jobExecHost)) {
            throw new IllegalStateException("No exec host found for " + jobInfo);
        } else {
            return Arrays.asList(jobExecHost.split("\\:"))
                    .stream()
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
                    .findFirst()
                    .map(execHost -> DEFAULT_SPARK_URI_SCHEME + "://" + execHost + ":" + DEFAULT_SPARK_MASTER_PORT)
                    .orElseThrow(() -> {
                        logger.error("No proper exec hosts has been set for {} even though exec host field is set to {}", jobInfo, jobExecHost);
                        return new IllegalStateException("No proper exec hosts has been set for " + jobInfo + " even though exec host field is set to " + jobExecHost);
                    });
        }
    }

    private boolean checkMininimumWorkerRequirement(SparkCluster sparkCluster) {
        List<JobInfo> workerJobs =
                sparkCluster.getWorkerJobIds().stream()
                        .map(jobId -> {
                            Collection<JobInfo> jobInfos = jobMgr.getJobInfo(sparkCluster.getMasterJobId());
                            return jobInfos.stream().findFirst()
                                    .orElseGet(() -> new JobInfoBuilder().setJobId(jobId).setStatus(JobStatus.PENDING).build());
                        })
                        .filter(jobInfo -> !jobInfo.isComplete()) // filter out completed jobs
                        .collect(Collectors.toList());
        if (workerJobs.size() == 0) {
            logger.error("No worker available on spark cluster {}", sparkCluster.getMasterJobId());
            throw new IllegalStateException("No worker available on spark cluster" + sparkCluster.getMasterJobId());
        } else if (workerJobs.size() < sparkCluster.getMinRequiredWorkers()) {
            logger.error("Not enough workers available on spark cluster {}, requested {} but only {} could be potentially become available",
                    sparkCluster.getMasterJobId(),
                    sparkCluster.getMinRequiredWorkers(),
                    workerJobs);
            throw new IllegalStateException("The minimum worker requirement for " + sparkCluster.getMasterJobId() + " cannot be met; " +
                    "requested " + sparkCluster.getMinRequiredWorkers() + " but only " + workerJobs.size() + " could become available");
        }
        return workerJobs.stream().filter(jobInfo -> jobInfo.getStatus() == JobStatus.RUNNING).count() >= sparkCluster.getMinRequiredWorkers();
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
                    if (jobInstanceInfo.getExitCode() != 143) { // We expect the cluster to be closed by us (TERM_OWNER)
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

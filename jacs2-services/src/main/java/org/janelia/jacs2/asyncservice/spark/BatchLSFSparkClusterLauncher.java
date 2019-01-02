package org.janelia.jacs2.asyncservice.spark;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.cluster.JobFuture;
import org.janelia.cluster.JobInfo;
import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobStatus;
import org.janelia.cluster.JobTemplate;
import org.janelia.cluster.lsf.LsfJobInfo;
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Start an LSF Spark cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class BatchLSFSparkClusterLauncher {

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
    public BatchLSFSparkClusterLauncher(ServiceComputationFactory computationFactory,
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
                                        @StrPropertyValue(name = "service.spark.lsf.batchApplication", defaultValue="sparkbatch32") String lsfApplication,
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
                                                         Path jobWorkingPath,
                                                         String billingInfo,
                                                         String sparkDriverMemory,
                                                         String sparkExecutorMemory,
                                                         String sparkLogConfigFile) {
        return submitClusterJob(numNodes, jobWorkingPath, billingInfo)
                .thenCompose(jobId -> createCluster(jobId, calculateDefaultParallelism(numNodes), sparkDriverMemory, sparkExecutorMemory, sparkLogConfigFile))
                ;
    }

    private ServiceComputation<Long> submitClusterJob(int numNodes, Path jobWorkingPath, String billingInfo) {
        int numSlots = nodeSlots + nodeSlots * numNodes; // master + workers

        logger.info("Starting Spark-{} cluster with one master + {} worker nodes ({} total slots)", sparkVersion, numNodes, numSlots);
        logger.info("Working directory: {}", jobWorkingPath);

        JobTemplate jt = new JobTemplate();
        jt.setJobName("sparkjacs");
        jt.setArgs(Collections.emptyList());
        jt.setWorkingDir(jobWorkingPath.toString());
        jt.setRemoteCommand(lsfRemoteCommand);
        jt.setNativeSpecification(createNativeSpec(billingInfo, numSlots));

        Long jobId;
        try {
            JobFuture future = jobMgr.submitJob(jt);
            jobId = future.getJobId();
            future.whenCompleteAsync((jobInfos, exc) -> {
                logJobInfo(jobInfos);
            }, COMPLETION_MESSAGE_EXECUTOR);
            logger.info("Submitted cluster job {} ", jobId);
            return computationFactory.newCompletedComputation(jobId);
        } catch (Exception e) {
            logger.error("Error starting a spark cluster with {} nodes ", numNodes, e);
            return computationFactory.newFailedComputation(e);
        }
    }

    int calculateDefaultParallelism(int numNodes) {
        // Default to three tasks per slot (this seems empirically optimal)
        return 3 * (nodeSlots / defaultCoresPerSparkExecutor) * numNodes;
    }

    ServiceComputation<SparkCluster> createCluster(Long clusterJobId,
                                                   int defaultParallelism,
                                                   String sparkDriverMemory,
                                                   String sparkExecutorMemory,
                                                   String sparkLogConfigFile) {
        return computationFactory.newCompletedComputation(clusterJobId)
                .thenSuspendUntil(
                        (Long aJobId) -> {
                            logger.trace("Check if spark cluster {} is ready", clusterJobId);
                            Collection<JobInfo> jobInfos = jobMgr.getJobInfo(aJobId);
                            if (CollectionUtils.isEmpty(jobInfos)) {
                                return null;
                            } else {
                                if (CollectionUtils.size(jobInfos) > 1) {
                                    logger.warn("More than one job found for Spark cluster {} - {} ", clusterJobId, jobInfos);
                                }
                                return (LsfJobInfo) jobInfos.stream().findFirst().orElse(null);
                            }
                        },
                        (LsfJobInfo jobInfo) -> {
                            if (jobInfo != null) {
                                if (jobInfo.isComplete()) {
                                    logger.error("Job {} for starting the spark cluster has already completed so nothing can be submitted to the cluster", jobInfo);
                                    throw new IllegalStateException("Spark cluster job " + jobInfo.getJobId() + " has already completed before starting the application");
                                }
                                if (jobInfo.getStatus() == JobStatus.RUNNING) {
                                    return new ContinuationCond.Cond<>(jobInfo, true);
                                } else {
                                    return new ContinuationCond.Cond<>(jobInfo, false);
                                }
                            } else {
                                return new ContinuationCond.Cond<>(null, false);
                            }
                        },
                        clusterIntervalCheckInMillis,
                        clusterStartTimeoutInMillis)
                .thenApply(lsfJobInfo -> {
                    List<String> clusterExecHosts = lsfJobInfo.getExecHosts();
                    String masterURI = clusterExecHosts
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
                                logger.error("No exec hosts has been set for {}", lsfJobInfo);
                                return new IllegalStateException("No exec host found for " + lsfJobInfo.toString());
                            });
                    logger.info("Spark cluster {} with master {} is running on the following hosts {}", clusterJobId, masterURI, clusterExecHosts);
                    return new SparkCluster(
                            computationFactory,
                            jobMgr,
                            clusterJobId,
                            ImmutableSet.of(clusterJobId),
                            1,
                            masterURI,
                            sparkHomeDir,
                            hadoopHomeDir,
                            StringUtils.defaultIfBlank(sparkDriverMemory, defaultSparkDriverMemory),
                            StringUtils.defaultIfBlank(sparkExecutorMemory, defaultSparkExecutorMemory),
                            defaultCoresPerSparkExecutor,
                            defaultParallelism,
                            StringUtils.defaultIfBlank(sparkLogConfigFile, defaultSparkLogConfigFile),
                            logger
                    );
                })
                .exceptionally(exc -> {
                    try {
                        // this may happen if starting the cluster timed out
                        logger.error("Killing job cluster {} because of an exception", clusterJobId, exc);
                        jobMgr.killJob(clusterJobId);
                    } catch (Exception ignore) {
                        logger.warn("Exception trying to kill {}", clusterJobId, ignore);
                    }
                    throw new IllegalStateException(exc);
                })
                ;
    }

    private List<String> createNativeSpec(String billingAccount, int slots) {
        List<String> spec = new ArrayList<>();
        spec.add("-a " + String.format("%s(%s)", lsfApplication, sparkVersion));
        spec.add("-n "+slots);
        spec.add("-W  "+ sparkClusterHardDurationMins);

        if (requiresAccountInfo)
            spec.add("-P "+billingAccount);

        return spec;
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

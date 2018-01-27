package org.janelia.jacs2.asyncservice.common.spark;

import com.google.common.base.Stopwatch;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.janelia.cluster.*;
import org.janelia.cluster.lsf.LsfJobInfo;
import org.janelia.jacs2.asyncservice.common.cluster.LsfParseUtils;
import org.janelia.jacs2.asyncservice.common.cluster.MonitoredJobManager;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.model.service.JacsJobInstanceInfo;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Manages a single Spark cluster with N nodes. Takes care of starting and stopping the master/worker nodes,
 * as well as submitting Spark applications to run on the cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SparkCluster {

    private static final String DRIVER_LOG_FILENAME = "driver.out";

    private static final ExecutorService completionMessageExecutor = Executors.newCachedThreadPool((runnable) -> {
        Thread thread = Executors.defaultThreadFactory().newThread(runnable);
        // Ensure that we can shut down without these threads getting in the way
        thread.setName("SparkCompletionMessageThread");
        thread.setDaemon(true);
        return thread;
    });

    // Configuration
    private final Logger log;
    private final JobManager jobMgr;
    private int nodeSlots;
    private String sparkVersion;
    private String sparkHomeDir;
    private String sparkDriverMemory;
    private String sparkExecutorMemory;
    private int sparkExecutorCores;
    private int sparkClusterHardDurationMins;
    private String log4jFilepath;
    private String hadoopHomeDir;

    // State
    private File workingDirectory;
    private Long jobId;
    private String master;

    @Inject
    public SparkCluster(MonitoredJobManager jobMgr,
                        @IntPropertyValue(name = "service.spark.nodeSlots", defaultValue = 16) int nodeSlots,
                        @StrPropertyValue(name = "service.spark.sparkVersion", defaultValue = "2") String sparkVersion,
                        @StrPropertyValue(name = "service.spark.sparkHomeDir", defaultValue = "/misc/local/spark-2") String sparkHomeDir,
                        @StrPropertyValue(name = "service.spark.driver.memory", defaultValue = "1g") String sparkDriverMemory,
                        @StrPropertyValue(name = "service.spark.executor.memory", defaultValue = "75g") String sparkExecutorMemory,
                        @IntPropertyValue(name = "service.spark.executor.cores", defaultValue = 15) int sparkExecutorCores,
                        @IntPropertyValue(name = "service.spark.cluster.hard.duration.mins", defaultValue = 30) int sparkClusterHardDurationMins,
                        @StrPropertyValue(name = "service.spark.log4jconfig.filepath", defaultValue = "") String log4jFilepath,
                        @StrPropertyValue(name = "hadoop.homeDir") String hadoopHomeDir,
                        Logger log) {
        this.jobMgr = jobMgr.getJobMgr();
        this.nodeSlots = nodeSlots;
        this.sparkVersion = sparkVersion;
        this.sparkHomeDir = sparkHomeDir;
        this.sparkDriverMemory = sparkDriverMemory;
        this.sparkExecutorMemory = sparkExecutorMemory;
        this.sparkExecutorCores = sparkExecutorCores;
        this.sparkClusterHardDurationMins = sparkClusterHardDurationMins;
        this.log4jFilepath = log4jFilepath;
        this.hadoopHomeDir = hadoopHomeDir;
        this.log = log;
    }

    private void reset() {
        this.workingDirectory = null;
        this.jobId = null;
        this.master = null;
    }

    public synchronized void startCluster(Path workingDirName, int numNodes) throws Exception {

        this.workingDirectory = workingDirName.toFile();
        int numSlots = nodeSlots + nodeSlots * numNodes; // master + workers

        log.info("Starting Spark cluster with {} nodes", nodeSlots);
        log.info("Working directory: {}", workingDirectory);

        JobTemplate jt = new JobTemplate();
        jt.setJobName("sparkbatch");
        jt.setArgs(Collections.emptyList());
        jt.setWorkingDir(workingDirectory.getAbsolutePath());
        jt.setRemoteCommand("commandstring");
        jt.setNativeSpecification(createNativeSpec(numSlots));

        final JobFuture future = jobMgr.submitJob(jt);
        this.jobId = future.getJobId();

        future.whenCompleteAsync((infos, e) -> {
            logJobInfo(infos);
            processJobCompletion(e);
        }, completionMessageExecutor);

        log.info("Starting Spark cluster {}", jobId);
    }

    public Collection<JacsJobInstanceInfo> getJobInstanceInfos(Collection<JobInfo> infos) {
        Collection<JacsJobInstanceInfo> jacsInfos = new ArrayList<>();
        for (JobInfo jobInfo : infos) {
            JacsJobInstanceInfo jacsJobInstanceInfo = new JacsJobInstanceInfo();
            jacsJobInstanceInfo.setJobId(jobInfo.getJobId());
            jacsJobInstanceInfo.setArrayIndex(jobInfo.getArrayIndex());
            jacsJobInstanceInfo.setName(jobInfo.getName());
            jacsJobInstanceInfo.setFromHost(jobInfo.getFromHost());
            jacsJobInstanceInfo.setExecHost(jobInfo.getExecHost());
            jacsJobInstanceInfo.setStatus(jobInfo.getStatus() == null ? null : jobInfo.getStatus().name());
            jacsJobInstanceInfo.setQueue(jobInfo.getQueue());
            jacsJobInstanceInfo.setProject(jobInfo.getProject());
            jacsJobInstanceInfo.setReqSlot(jobInfo.getReqSlot());
            jacsJobInstanceInfo.setAllocSlot(jobInfo.getAllocSlot());
            jacsJobInstanceInfo.setSubmitTime(LsfParseUtils.convertLocalDateTime(jobInfo.getSubmitTime()));
            jacsJobInstanceInfo.setStartTime(LsfParseUtils.convertLocalDateTime(jobInfo.getStartTime()));
            jacsJobInstanceInfo.setFinishTime(LsfParseUtils.convertLocalDateTime(jobInfo.getFinishTime()));
            jacsJobInstanceInfo.setQueueSecs(LsfParseUtils.getDiffSecs(jobInfo.getSubmitTime(), jobInfo.getStartTime()));
            jacsJobInstanceInfo.setRunSecs(LsfParseUtils.getDiffSecs(jobInfo.getStartTime(), jobInfo.getFinishTime()));
            jacsJobInstanceInfo.setMaxMem(jobInfo.getMaxMem());
            jacsJobInstanceInfo.setMaxMemBytes(LsfParseUtils.parseMemToBytes(jobInfo.getMaxMem()));
            jacsJobInstanceInfo.setExitCode(jobInfo.getExitCode());
            jacsJobInstanceInfo.setExitReason(jobInfo.getExitReason());
            jacsInfos.add(jacsJobInstanceInfo);
        }
        return jacsInfos;
    }

    private void logJobInfo(Collection<JobInfo> infos) {
        for (JacsJobInstanceInfo jobInfo : getJobInstanceInfos(infos)) {
            Long queueTimeSeconds = jobInfo.getQueueSecs();
            Long runTimeSeconds = jobInfo.getRunSecs();

            String queueTime = queueTimeSeconds+" sec";
            if (queueTimeSeconds!=null && queueTimeSeconds>300) { // More than 5 minutes, just show the minutes
                queueTime = TimeUnit.MINUTES.convert(queueTimeSeconds, TimeUnit.SECONDS) + " min";
            }

            String runTime = runTimeSeconds+" sec";
            if (runTimeSeconds!=null && runTimeSeconds>300) {
                runTime = TimeUnit.MINUTES.convert(runTimeSeconds, TimeUnit.SECONDS) + " min";
            }

            String maxMem = jobInfo.getMaxMem();
            String jobIdStr = jobInfo.getJobId()+"";
            if (jobInfo.getArrayIndex()!=null) {
                jobIdStr += "."+jobInfo.getArrayIndex();
            }

            log.info("Job {} was queued for {}, ran for {}, and used "+maxMem+" of memory.", jobIdStr, queueTime, runTime);
            if (jobInfo.getExitCode()!=143) { // We expect the cluster to be closed by us (TERM_OWNER)
                log.error("Job {} exited with code {} and reason {}", jobIdStr, jobInfo.getExitCode(), jobInfo.getExitReason());
            }
        }
    }

    private synchronized void processJobCompletion(Throwable exception) {
        if (exception != null) {
            log.error("Spark cluster ended with error",exception);
        }
        if (jobId!=null) {
            log.warn("Spark cluster {} has died", jobId);
            reset();
        }
    }

    private List<String> createNativeSpec(int slots) {
        String jobName = "sparkbatch";
        List<String> spec = new ArrayList<>();
        spec.add("-a sparkbatch("+ sparkVersion +")");
        spec.add("-n "+slots);
        spec.add("-W  "+ sparkClusterHardDurationMins);
        return spec;
    }

    public synchronized boolean isReady() {

        if (master != null) return true;

        if (jobId == null) {
            throw new IllegalStateException("Spark cluster has no associated job id");
        }

        log.debug("Checking if Spark cluster {} is ready", jobId);

        Collection<JobInfo> jobInfos = jobMgr.getJobInfo(jobId);
        if (jobInfos!=null && !jobInfos.isEmpty()) {

            if (jobInfos.size()>1) {
                log.warn("More than one job found for Spark cluster with id "+jobId);
            }

            JobInfo jobInfo = jobInfos.iterator().next();
            if (jobInfo.getStatus()==JobStatus.RUNNING) {
                List<String> execHosts = ((LsfJobInfo)jobInfo).getExecHosts();
                log.debug("Got exec hosts: {}",execHosts);
                if (execHosts!=null) {
                    for(String host : execHosts) {
                        if (host.contains("*")) {
                            host = host.split("\\*")[1];
                        }
                        this.master = "spark://"+host+":7077";
                        log.info("Spark cluster {} is running on the following hosts: {}", jobId, execHosts);
                        log.info("Spark cluster {} is now ready with master on {}", jobId, master);
                        break; // We only care about the master
                    }
                }
            }
        }

        return master != null;
    }

    /**
     * Run the app given by the specified jar file on the currently running cluster. If the cluster is not ready an
     * IllegalStateException will be thrown. Command line arguments can be passed through via appArgs arguments.
     * @param callback called when the application has ended
     * @param jarPath absolute path to a far jar file containing the  with dependencies
     * @param appArgs
     * @return
     * @throws Exception
     */
    public SparkApp runApp(BiConsumer<File, ? super Throwable> callback, String jarPath, String... appArgs) throws Exception {

        if (!isReady()) {
            throw new IllegalStateException("Cluster is not ready");
        }

        Stopwatch s = Stopwatch.createStarted();

        log.info("Running Spark app on cluster {}", master);
        File outFile = new File(workingDirectory, DRIVER_LOG_FILENAME);
        log.info("Driver output file: {}", outFile);

        SparkAppHandle.Listener listener = new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle sparkAppHandle) {
                SparkAppHandle.State state = sparkAppHandle.getState();
                log.info("Spark application state changed: "+state);
                if (state.isFinal()) {
                    long seconds = s.stop().elapsed().getSeconds();
                    log.info("Spark application ran for {} seconds.", seconds);
                    if (state==SparkAppHandle.State.FINISHED) {
                        if (callback!=null) callback.accept(workingDirectory, null);
                    }
                    else {
                        if (callback!=null) callback.accept(workingDirectory, new Exception("Spark application finished with error state: "+state));
                    }
                }
            }

            @Override
            public void infoChanged(SparkAppHandle sparkAppHandle) {
                log.info("Spark application id: "+sparkAppHandle.getAppId());
            }
        };

        File log4jProperties = new File(log4jFilepath);

        SparkAppHandle handle = new SparkLauncher()
                .redirectError()
                .redirectOutput(outFile)
                .setAppResource(jarPath)
                .setMaster(master)
                .setSparkHome(sparkHomeDir)
                .setConf("spark.ui.showConsoleProgress", "false") // The console progress bar screws up the STDOUT stream
                .setConf(SparkLauncher.DRIVER_MEMORY, sparkDriverMemory)
                .setConf(SparkLauncher.EXECUTOR_MEMORY, sparkExecutorMemory)
                .setConf(SparkLauncher.EXECUTOR_CORES, ""+sparkExecutorCores)
                .setConf(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS, "-Dlog4j.configuration=file://"+log4jProperties.getAbsolutePath())
                .addSparkArg("--driver-java-options",
                        "-Dlog4j.configuration=file://"+log4jProperties.getAbsolutePath()+
                        " -Dhadoop.home.dir="+ hadoopHomeDir +
                        " -Djava.library.path="+ hadoopHomeDir +"/lib/native"
                        )
                .addAppArgs(appArgs)
                .startApplication(listener);

        // Currently, if the process fails here, then we don't get any notification. We'll have to wait until time out.
        // This bug is fixed in Spark 2.3, but unfortunately not backported to 2.2.x. We should upgrade to 2.3 as soon
        // as it's available.

        return new SparkApp(this, handle);
    }

    /**
     * Stop the cluster by killing the job. This must always be called before this job is completed.
     */
    public synchronized void stopCluster() {

        if (jobId==null) {
            throw new IllegalStateException("Cluster is not running");
        }

        log.info("Stopping Spark cluster {}", jobId);

        try {
            jobMgr.killJob(jobId);
        }
        catch (Exception e) {
            log.error("Error stopping Spark cluster", e);
        }
        reset();
    }
}

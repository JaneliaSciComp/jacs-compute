package org.janelia.jacs2.asyncservice.common.spark;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.janelia.cluster.*;
import org.janelia.cluster.lsf.LsfJobInfo;
import org.janelia.jacs2.asyncservice.common.cluster.MonitoredJobManager;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
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
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Manages a single Spark cluster with N nodes. Takes care of starting and stopping the master/worker nodes,
 * as well as submitting Spark applications to run on the cluster.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SparkCluster {

    private static final ExecutorService completionMessageExecutor = Executors.newCachedThreadPool((runnable) -> {
        Thread thread = Executors.defaultThreadFactory().newThread(runnable);
        // Ensure that we can shut down without these threads getting in the way
        thread.setName("SparkCompletionMessageThread");
        thread.setDaemon(true);
        return thread;
    });

    private static final int NODE_SLOTS = 16;
    private static final String SPARK_VERSION = "2";
    private static final String SPARK_HOME = "/misc/local/spark-2";

    private static final String SPARK_DRIVER_MEMORY = "1g";
    private static final String SPARK_EXECUTOR_MEMORY = "75g";
    private static final String SPARK_EXECUTOR_CORES = "15";


    private static final long hardJobDurationInMins = 30;
    private static final String HADOOP_HOME = "/misc/local/hadoop-2.6.4";

    private final Logger log;
    private final JacsServiceDataPersistence jacsServiceDataPersistence;
    private final JobManager jobMgr;

    private File workingDirectory;
    private Long jobId;
    private String master;

    @Inject
    public SparkCluster(MonitoredJobManager jobMgr, JacsServiceDataPersistence jacsServiceDataPersistence, Logger log) {
        this.jobMgr = jobMgr.getJobMgr();
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
        this.log = log;
    }

    private void reset() {
        this.workingDirectory = null;
        this.jobId = null;
        this.master = null;
    }

    public synchronized void startCluster(Path workingDirName, int numNodes) throws Exception {

        this.workingDirectory = workingDirName.toFile();
        int numSlots = NODE_SLOTS + NODE_SLOTS * numNodes; // master + workers

        log.info("Starting Spark cluster with {} nodes", NODE_SLOTS);
        log.info("Working directory: {}", workingDirectory);

        JobTemplate jt = new JobTemplate();
        jt.setJobName("sparkbatch");
        jt.setArgs(Collections.emptyList());
        jt.setWorkingDir(workingDirectory.getAbsolutePath());
        jt.setRemoteCommand("commandstring");
        jt.setNativeSpecification(createNativeSpec(numSlots));

        final JobFuture future = jobMgr.submitJob(jt, 1, 1);
        this.jobId = future.getJobId();

        future.whenCompleteAsync((infos, e) -> {
            processJobCompletion(e);
        }, completionMessageExecutor);

        log.info("Starting Spark cluster {}", jobId);
    }

    private synchronized void processJobCompletion(Throwable exception) {
        if (exception != null) {
            log.error("Spark cluster ended with error",exception);
        }
        if (jobId!=null) {
            log.error("Spark cluster was not stopped cleanly");
            reset();
        }
    }

    private List<String> createNativeSpec(int slots) {
        String jobName = "sparkbatch";
        List<String> spec = new ArrayList<>();
        spec.add("-a sparkbatch("+SPARK_VERSION+")");
        spec.add("-n "+slots);
        spec.add("-W  "+hardJobDurationInMins);
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
                        log.info("Spark cluster {} is now ready with master on {}", jobId, master);
                        break; // We only care about the master
                    }
                }
            }
        }

        return master != null;
    }

    public SparkApp runApp(BiConsumer<File, ? super Throwable> callback, String jarPath, String... appArgs) throws Exception {

        if (!isReady()) {
            throw new IllegalStateException("Cluster is not ready");
        }

        log.info("Running Spark app on cluster {}", master);
        File outFile = new File(workingDirectory, "driver.out");
        log.info("Driver output file: {}", outFile);

        SparkAppHandle.Listener listener = new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(SparkAppHandle sparkAppHandle) {
                SparkAppHandle.State state = sparkAppHandle.getState();
                log.info("Spark application state changed: "+state);
                if (state.isFinal()) {
                    log.info("Spark application completed");
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

        File log4jProperties = new File("/home/rokickik/dev/colormipsearch/log4j.properties");

        SparkAppHandle handle = new SparkLauncher()
                .redirectError()
                .redirectOutput(outFile)
                .setAppResource(jarPath)
                .setMaster(master)
                .setSparkHome(SPARK_HOME)
//                .addFile(log4jProperties.getAbsolutePath())
                .setConf("spark.ui.showConsoleProgress", "false") // The console progress bar screws up the STDOUT stream
                .setConf(SparkLauncher.DRIVER_MEMORY, SPARK_DRIVER_MEMORY)
                .setConf(SparkLauncher.EXECUTOR_MEMORY, SPARK_EXECUTOR_MEMORY)
                .setConf(SparkLauncher.EXECUTOR_CORES, SPARK_EXECUTOR_CORES)
                .setConf(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS, "-Dlog4j.configuration=log4j.properties")
                .addSparkArg("--driver-java-options",
                        " -Dlog4j.configuration="+log4jProperties.getAbsolutePath()+
                        "-Dhadoop.home.dir="+HADOOP_HOME+
                        " -Djava.library.path="+HADOOP_HOME+"/lib/native"
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

package org.janelia.jacs2.asyncservice.common;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class ThrottledExeJobsQueue {

    private final int initialDelayInMillis;
    private final int periodInMillis;
    private Logger logger;
    private ScheduledExecutorService scheduler;
    private Map<String, BlockingQueue<ThrottledJobInfo>> waitingJobs;
    private Map<String, BlockingQueue<ThrottledJobInfo>> runningJobs;

    ThrottledExeJobsQueue() {
        // CDI required ctor
        this.initialDelayInMillis = 30000;
        this.periodInMillis = 500;
    }

    @Inject
    public ThrottledExeJobsQueue(Logger logger) {
        this();
        this.logger = logger;
    }

    @PostConstruct
    public void initialize() {
        waitingJobs = new ConcurrentHashMap<>();;
        runningJobs = new ConcurrentHashMap<>();;
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("JACS-THROTTLE-%d")
                .setDaemon(true)
                .build();
        scheduler = Executors.newScheduledThreadPool(1, threadFactory);
        scheduler.scheduleAtFixedRate(() -> checkWaitingQueue(), initialDelayInMillis, periodInMillis, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void destroy() {
        scheduler.shutdownNow();
    }

    /**
     * This method adds another job to the queue either as a running job or as a waiting job if there are not enough resources.
     * Since this can be called by multiple threads the access to it needs to be synchronized
     * @param jobInfo being added
     * @return
     */
    synchronized String add(ThrottledJobInfo jobInfo) {
        if (jobInfo.getMaxRunningProcesses() <= 0 ||
                CollectionUtils.size(runningJobs.get(jobInfo.getJobType())) < jobInfo.getMaxRunningProcesses()) {
            addJobDoneCallback(jobInfo);
            String jobId = jobInfo.beginProcessing();
            if (jobInfo.getMaxRunningProcesses() > 0) {
                BlockingQueue<ThrottledJobInfo> queue = getQueue(jobInfo.getJobType(), runningJobs);
                queue.add(jobInfo);
            }
            return jobId;
        } else {
            BlockingQueue<ThrottledJobInfo> queue = getQueue(jobInfo.getJobType(), waitingJobs);
            queue.add(jobInfo);
            return null;
        }
    }

    private BlockingQueue<ThrottledJobInfo> getQueue(String name, Map<String, BlockingQueue<ThrottledJobInfo>> whichProcesses) {
        synchronized (whichProcesses) {
            BlockingQueue<ThrottledJobInfo> queue = whichProcesses.get(name);
            if (queue == null) {
                queue = new LinkedBlockingQueue<>();
                whichProcesses.put(name, queue);
            }
            return queue;
        }
    }

    private void moveProcessToRunningQueue(ThrottledJobInfo jobInfo) {
        logger.debug("Prepare for actually running queue {} - {}", jobInfo.getJobType(), jobInfo.getJobServiceContext());
        BlockingQueue<ThrottledJobInfo> waitingQueue = getQueue(jobInfo.getJobType(), waitingJobs);
        BlockingQueue<ThrottledJobInfo> runningQueue = getQueue(jobInfo.getJobType(), runningJobs);
        waitingQueue.remove(jobInfo);
        if (jobInfo.beginProcessing() != null) {
            runningQueue.add(jobInfo);
        }
    }

    private void removeProcessFromRunningQueue(ThrottledJobInfo jobInfo) {
        BlockingQueue<ThrottledJobInfo> runningQueue = getQueue(jobInfo.getJobType(), runningJobs);
        boolean removed = runningQueue.remove(jobInfo);
        if (removed) {
            logger.debug("Completed {}:{} and removed it from the runningQueue (size={})", jobInfo.getJobType(), jobInfo.getScriptName(), runningQueue.size());
        }
        else {
            logger.debug("Completed {}:{} and failed to remote it from the runningQueue (size={})", jobInfo.getJobType(), jobInfo.getScriptName(), runningQueue.size());
        }
    }

    private void checkWaitingQueue() {
        for (Map.Entry<String, BlockingQueue<ThrottledJobInfo>> queueEntry : waitingJobs.entrySet()) {
            BlockingQueue<ThrottledJobInfo> queue = queueEntry.getValue();
            for (ThrottledJobInfo jobInfo = queue.poll(); jobInfo != null; jobInfo = queue.poll()) {
                if (CollectionUtils.size(runningJobs.get(jobInfo.getJobType())) < jobInfo.getMaxRunningProcesses()) {
                    logger.debug("Move {} - {} to running queue", jobInfo.getJobType(), jobInfo.getJobServiceContext());
                    addJobDoneCallback(jobInfo);
                    moveProcessToRunningQueue(jobInfo);
                    jobInfo.beginProcessing();
                } else {
                    queue.add(jobInfo);
                    break;
                }
            }
        }
    }

    private void addJobDoneCallback(ThrottledJobInfo jobInfo) {
        jobInfo.setJobDoneCallback(ji -> removeProcessFromRunningQueue(ji));
    }
}

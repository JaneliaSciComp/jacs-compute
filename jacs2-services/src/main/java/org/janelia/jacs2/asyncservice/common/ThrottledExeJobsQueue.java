package org.janelia.jacs2.asyncservice.common;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;

@Dependent
public class ThrottledExeJobsQueue {

    private final int initialDelayInMillis;
    private final int periodInMillis;
    private Logger logger;
    private ScheduledExecutorService scheduler;
    private Map<String, BlockingQueue<ThrottledExeJobHandler>> waitingJobs;
    private Map<String, BlockingQueue<ThrottledExeJobHandler>> runningJobs;

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
        logger.info("Initialize throttled jobs executor to run every {}ms with an initial delay of {}ms",
                periodInMillis, initialDelayInMillis);
        waitingJobs = new ConcurrentHashMap<>();;
        runningJobs = new ConcurrentHashMap<>();;
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("JACS-THROTTLE-%d")
                .setDaemon(true)
                .build();
        scheduler = Executors.newScheduledThreadPool(1, threadFactory);
        scheduler.scheduleAtFixedRate(this::checkWaitingQueue, initialDelayInMillis, periodInMillis, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void destroy() {
        logger.info("Shutdown throttled jobs executor");
        scheduler.shutdownNow();
    }

    /**
     * This method adds another job to the queue either as a running job or as a waiting job if there are not enough resources.
     * Since this can be called by multiple threads the access to it needs to be synchronized
     * @param jobInfo being added
     * @return
     */
    synchronized void add(ThrottledExeJobHandler jobInfo) {
        if (jobInfo.getMaxRunningProcesses() <= 0) {
            addJobDoneCallback(jobInfo);
            jobInfo.beginProcessing();
            // there are no restrictions on the number of running processes so no need to enqueue the job
        } else if (CollectionUtils.size(runningJobs.get(jobInfo.getJobType())) < jobInfo.getMaxRunningProcesses()) {
            addJobDoneCallback(jobInfo);
            jobInfo.beginProcessing();
            // there restrictions on the number of running processes so "mark" the job as running
            BlockingQueue<ThrottledExeJobHandler> runningJobsQueue = getQueue(jobInfo.getJobType(), runningJobs);
            runningJobsQueue.add(jobInfo);
        } else {
            // no space left
            BlockingQueue<ThrottledExeJobHandler> waitingJobsQueue = getQueue(jobInfo.getJobType(), waitingJobs);
            waitingJobsQueue.add(jobInfo);
        }
    }

    private BlockingQueue<ThrottledExeJobHandler> getQueue(String name, Map<String, BlockingQueue<ThrottledExeJobHandler>> whichProcesses) {
        synchronized (whichProcesses) {
            BlockingQueue<ThrottledExeJobHandler> queue = whichProcesses.get(name);
            if (queue == null) {
                queue = new LinkedBlockingQueue<>();
                whichProcesses.put(name, queue);
            }
            return queue;
        }
    }

    private void moveProcessToRunningQueue(ThrottledExeJobHandler jobInfo) {
        logger.debug("Prepare for actually running queue {} - {}", jobInfo.getJobType(), jobInfo.getJobServiceContext());
        BlockingQueue<ThrottledExeJobHandler> waitingQueue = getQueue(jobInfo.getJobType(), waitingJobs);
        BlockingQueue<ThrottledExeJobHandler> runningQueue = getQueue(jobInfo.getJobType(), runningJobs);
        waitingQueue.remove(jobInfo);
        runningQueue.add(jobInfo);
    }

    private void removeProcessFromRunningQueue(ThrottledExeJobHandler jobInfo) {
        BlockingQueue<ThrottledExeJobHandler> runningQueue = getQueue(jobInfo.getJobType(), runningJobs);
        boolean removed = runningQueue.remove(jobInfo);
        if (removed) {
            logger.debug("Completed {}:{} and removed it from the runningQueue (size={})", jobInfo.getJobType(), jobInfo.getJobInfo(), runningQueue.size());
        }
        else {
            logger.debug("Completed {}:{} and failed to remote it from the runningQueue (size={})", jobInfo.getJobType(), jobInfo.getJobInfo(), runningQueue.size());
        }
    }

    private void checkWaitingQueue() {
        for (Map.Entry<String, BlockingQueue<ThrottledExeJobHandler>> queueEntry : waitingJobs.entrySet()) {
            BlockingQueue<ThrottledExeJobHandler> queue = queueEntry.getValue();
            for (ThrottledExeJobHandler jobInfo = queue.poll(); jobInfo != null; jobInfo = queue.poll()) {
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

    private void addJobDoneCallback(ThrottledExeJobHandler jobInfo) {
        jobInfo.setJobDoneCallback(ji -> removeProcessFromRunningQueue(ji));
    }
}

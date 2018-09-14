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
    private Map<String, BlockingQueue<ThrottledJobHandler>> waitingJobs;
    private Map<String, BlockingQueue<ThrottledJobHandler>> runningJobs;

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
    synchronized void add(ThrottledJobHandler jobInfo) {
        if (jobInfo.getMaxRunningProcesses() <= 0) {
            addJobDoneCallback(jobInfo);
            jobInfo.beginProcessing();
            // there are no restrictions on the number of running processes so no need to enqueue the job
        } else if (CollectionUtils.size(runningJobs.get(jobInfo.getJobType())) < jobInfo.getMaxRunningProcesses()) {
            addJobDoneCallback(jobInfo);
            jobInfo.beginProcessing();
            // there restrictions on the number of running processes so "mark" the job as running
            BlockingQueue<ThrottledJobHandler> runningJobsQueue = getQueue(jobInfo.getJobType(), runningJobs);
            runningJobsQueue.add(jobInfo);
        } else {
            // no space left
            BlockingQueue<ThrottledJobHandler> waitingJobsQueue = getQueue(jobInfo.getJobType(), waitingJobs);
            waitingJobsQueue.add(jobInfo);
        }
    }

    private BlockingQueue<ThrottledJobHandler> getQueue(String name, Map<String, BlockingQueue<ThrottledJobHandler>> whichProcesses) {
        synchronized (whichProcesses) {
            BlockingQueue<ThrottledJobHandler> queue = whichProcesses.get(name);
            if (queue == null) {
                queue = new LinkedBlockingQueue<>();
                whichProcesses.put(name, queue);
            }
            return queue;
        }
    }

    private void moveProcessToRunningQueue(ThrottledJobHandler jobInfo) {
        logger.debug("Prepare for actually running queue {} - {}", jobInfo.getJobType(), jobInfo.getJobServiceContext());
        BlockingQueue<ThrottledJobHandler> waitingQueue = getQueue(jobInfo.getJobType(), waitingJobs);
        BlockingQueue<ThrottledJobHandler> runningQueue = getQueue(jobInfo.getJobType(), runningJobs);
        waitingQueue.remove(jobInfo);
        runningQueue.add(jobInfo);
    }

    private void removeProcessFromRunningQueue(ThrottledJobHandler jobInfo) {
        BlockingQueue<ThrottledJobHandler> runningQueue = getQueue(jobInfo.getJobType(), runningJobs);
        boolean removed = runningQueue.remove(jobInfo);
        if (removed) {
            logger.debug("Completed {}:{} and removed it from the runningQueue (size={})", jobInfo.getJobType(), jobInfo.getJobInfo(), runningQueue.size());
        }
        else {
            logger.debug("Completed {}:{} and failed to remote it from the runningQueue (size={})", jobInfo.getJobType(), jobInfo.getJobInfo(), runningQueue.size());
        }
    }

    private void checkWaitingQueue() {
        for (Map.Entry<String, BlockingQueue<ThrottledJobHandler>> queueEntry : waitingJobs.entrySet()) {
            BlockingQueue<ThrottledJobHandler> queue = queueEntry.getValue();
            for (ThrottledJobHandler jobInfo = queue.poll(); jobInfo != null; jobInfo = queue.poll()) {
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

    private void addJobDoneCallback(ThrottledJobHandler jobInfo) {
        jobInfo.setJobDoneCallback(ji -> removeProcessFromRunningQueue(ji));
    }
}

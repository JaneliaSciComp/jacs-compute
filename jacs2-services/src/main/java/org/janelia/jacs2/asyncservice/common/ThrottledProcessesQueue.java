package org.janelia.jacs2.asyncservice.common;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.collections4.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@Singleton
public class ThrottledProcessesQueue {

    private final ScheduledExecutorService scheduler;
    private final Map<String, BlockingQueue<ThrottledJobInfo>> waitingProcesses;
    private final Map<String, BlockingQueue<ThrottledJobInfo>> runningProcesses;
    private final int initialDelayInMillis;
    private final int periodInMillis;

    @Inject
    ThrottledProcessesQueue() {
        this.waitingProcesses = new ConcurrentHashMap<>();
        this.runningProcesses = new ConcurrentHashMap<>();
        this.initialDelayInMillis = 30000;
        this.periodInMillis = 1000;
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("JACS-THROTTLE-%d")
                .setDaemon(true)
                .build();
        this.scheduler = Executors.newScheduledThreadPool(1, threadFactory);
    }

    @PostConstruct
    public void initialize() {
        scheduler.scheduleAtFixedRate(() -> checkWaitingQueue(), initialDelayInMillis, periodInMillis, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void destroy() {
        scheduler.shutdownNow();
    }

    ThrottledJobInfo add(ThrottledJobInfo jobInfo) {
        if (jobInfo.getMaxRunningProcesses() <= 0 ||
                CollectionUtils.size(runningProcesses.get(jobInfo.getProcessName())) < jobInfo.getMaxRunningProcesses()) {
            jobInfo.setJobDoneCallback(ji -> removeProcessFromRunningQueue(ji));
            if (jobInfo.runProcess() && jobInfo.getMaxRunningProcesses() > 0) {
                BlockingQueue<ThrottledJobInfo> queue = getQueue(jobInfo.getProcessName(), runningProcesses);
                queue.add(jobInfo);
            }
        } else {
            BlockingQueue<ThrottledJobInfo> queue = getQueue(jobInfo.getProcessName(), waitingProcesses);
            queue.add(jobInfo);
        }
        return jobInfo;
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
        BlockingQueue<ThrottledJobInfo> waitingQueue = getQueue(jobInfo.getProcessName(), waitingProcesses);
        BlockingQueue<ThrottledJobInfo> runningQueue = getQueue(jobInfo.getProcessName(), runningProcesses);
        waitingQueue.remove(jobInfo);
        if (jobInfo.runProcess()) {
            runningQueue.add(jobInfo);
        }
    }

    private void removeProcessFromRunningQueue(ThrottledJobInfo jobInfo) {
        BlockingQueue<ThrottledJobInfo> runningQueue = getQueue(jobInfo.getProcessName(), runningProcesses);
        runningQueue.remove(jobInfo);
    }

    private void checkWaitingQueue() {
        for (Map.Entry<String, BlockingQueue<ThrottledJobInfo>> queueEntry : waitingProcesses.entrySet()) {
            BlockingQueue<ThrottledJobInfo> queue = queueEntry.getValue();
            for (ThrottledJobInfo jobInfo = queue.peek(); jobInfo != null; jobInfo = queue.peek()) {
                if (CollectionUtils.size(runningProcesses.get(jobInfo.getProcessName())) < jobInfo.getMaxRunningProcesses()) {
                    jobInfo.setJobDoneCallback(ji -> removeProcessFromRunningQueue(ji));
                    moveProcessToRunningQueue(jobInfo);
                    jobInfo.runProcess();
                } else {
                    break;
                }
            }
        }
    }

}

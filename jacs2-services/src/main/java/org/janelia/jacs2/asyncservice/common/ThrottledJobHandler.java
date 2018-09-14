package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsJobInstanceInfo;
import org.janelia.model.service.JacsServiceData;

import java.util.Collection;

public class ThrottledJobHandler implements JobHandler {

    public interface JobDoneCallback {
        void done(ThrottledJobHandler jobInfo);
    }

    private final JobHandler throttledJobInfo;
    private final JacsServiceData jobServiceContext;
    private final ThrottledExeJobsQueue jobsQueue;
    private final int maxRunningProcesses;
    private volatile boolean terminated;
    private JobDoneCallback jobDoneCallback;

    ThrottledJobHandler(JobHandler throttledJobInfo, JacsServiceData jobServiceContext, ThrottledExeJobsQueue jobsQueue, int maxRunningProcesses) {
        this.throttledJobInfo = throttledJobInfo;
        this.jobServiceContext = jobServiceContext;
        this.jobsQueue = jobsQueue;
        this.maxRunningProcesses = maxRunningProcesses;
    }

    @Override
    public String getJobInfo() {
        return throttledJobInfo.getJobInfo();
    }

    @Override
    public boolean start() {
        if (!terminated) {
            jobsQueue.add(this); // simply enqueue the job
            return true;
        } else
            return false;
    }

    @Override
    public boolean isDone() {
        boolean done = terminated || throttledJobInfo.isDone();
        if (done && jobDoneCallback != null) {
            jobDoneCallback.done(this);
        }
        return done;
    }

    @Override
    public Collection<JacsJobInstanceInfo> getJobInstances() {
        return throttledJobInfo.getJobInstances();
    }

    @Override
    public boolean hasFailed() {
        boolean failed = terminated || throttledJobInfo.hasFailed();
        if (failed && jobDoneCallback != null) {
            jobDoneCallback.done(this);
        }
        return failed;
    }

    @Override
    public void terminate() {
        terminated = true;
        throttledJobInfo.terminate();
        if (jobDoneCallback != null) {
            jobDoneCallback.done(this);
        }
    }

    JacsServiceData getJobServiceContext() {
        return jobServiceContext;
    }

    String getJobType() {
        return jobServiceContext.getName();
    }

    int getMaxRunningProcesses() {
        return maxRunningProcesses;
    }

    void setJobDoneCallback(JobDoneCallback jobDoneCallback) {
        this.jobDoneCallback = jobDoneCallback;
    }

    void beginProcessing() {
        throttledJobInfo.start();
    }

}

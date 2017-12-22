package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsJobInstanceInfo;
import org.janelia.model.service.JacsServiceData;

import java.util.Collection;

public class ThrottledJobInfo implements ExeJobInfo {

    public interface JobDoneCallback {
        void done(ThrottledJobInfo jobInfo);
    }

    private final ExeJobInfo throttledJobInfo;
    private final JacsServiceData jobServiceContext;
    private final ThrottledExeJobsQueue jobsQueue;
    private final int maxRunningProcesses;
    private volatile boolean terminated;
    private JobDoneCallback jobDoneCallback;

    ThrottledJobInfo(ExeJobInfo throttledJobInfo, JacsServiceData jobServiceContext, ThrottledExeJobsQueue jobsQueue, int maxRunningProcesses) {
        this.throttledJobInfo = throttledJobInfo;
        this.jobServiceContext = jobServiceContext;
        this.jobsQueue = jobsQueue;
        this.maxRunningProcesses = maxRunningProcesses;
    }

    @Override
    public String getScriptName() {
        return throttledJobInfo.getScriptName();
    }

    @Override
    public String start() {
        if (!terminated) {
            return jobsQueue.add(this); // simply enqueue the job
        } else
            return null;
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
    public Collection<JacsJobInstanceInfo> getJobInstanceInfos() {
        return throttledJobInfo.getJobInstanceInfos();
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

    String beginProcessing() {
        return throttledJobInfo.start();
    }

}

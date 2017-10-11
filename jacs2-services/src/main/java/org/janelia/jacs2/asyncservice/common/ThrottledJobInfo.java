package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.model.jacsservice.JacsServiceData;

import java.util.List;
import java.util.Map;

public class ThrottledJobInfo implements ExeJobInfo {

    public static interface JobDoneCallback {
        void done(ThrottledJobInfo jobInfo);
    }

    private final ExternalCodeBlock externalCode;
    private final List<ExternalCodeBlock> externalConfigs;
    private final Map<String, String> env;
    private final String workingDirName;
    private final JacsServiceData serviceContext;

    private final String processName;
    private final ExternalProcessRunner actualProcessRunner;
    private final int maxRunningProcesses;
    private ExeJobInfo actualRunningJobInfo;
    private volatile boolean doNotRun;
    private JobDoneCallback jobDoneCallback;

    public ThrottledJobInfo(ExternalCodeBlock externalCode, List<ExternalCodeBlock> externalConfigs, Map<String, String> env, String workingDirName, JacsServiceData serviceContext,
                            String processName, ExternalProcessRunner actualProcessRunner, int maxRunningProcesses) {
        this.externalCode = externalCode;
        this.externalConfigs = externalConfigs;
        this.env = env;
        this.workingDirName = workingDirName;
        this.serviceContext = serviceContext;
        this.processName = processName;
        this.actualProcessRunner = actualProcessRunner;
        this.maxRunningProcesses = maxRunningProcesses;
    }

    @Override
    public String getScriptName() {
        return actualRunningJobInfo != null ? actualRunningJobInfo.getScriptName() : null;
    }

    @Override
    public boolean isDone() {
        boolean done = checkIfDone();
        if (done && jobDoneCallback != null) {
            jobDoneCallback.done(this);
        }
        return done;
    }

    private boolean checkIfDone() {
        if (actualRunningJobInfo == null) {
            return doNotRun;
        } else {
            return actualRunningJobInfo.isDone();
        }

    }

    @Override
    public boolean hasFailed() {
        boolean failed = checkIfFailed();
        if (failed && jobDoneCallback != null) {
            jobDoneCallback.done(this);
        }
        return failed;
    }

    private boolean checkIfFailed() {
        if (actualRunningJobInfo == null) {
            return doNotRun;
        } else {
            return actualRunningJobInfo.hasFailed();
        }
    }

    @Override
    public void terminate() {
        if (actualRunningJobInfo != null) {
            actualRunningJobInfo.terminate();
        } else {
            doNotRun = true;
        }
        if (jobDoneCallback != null) {
            jobDoneCallback.done(this);
        }
    }

    boolean runProcess() {
        if (!doNotRun) {
            ExeJobInfo actualJobInfo = actualProcessRunner.runCmds(getExternalCode(), getExternalConfigs(), getEnv(), getWorkingDirName(), getServiceContext());
            setActualRunningJobInfo(actualJobInfo);
            return true;
        } else
            return false;
    }

    ExternalCodeBlock getExternalCode() {
        return externalCode;
    }

    List<ExternalCodeBlock> getExternalConfigs() {
        return externalConfigs;
    }

    Map<String, String> getEnv() {
        return env;
    }

    String getWorkingDirName() {
        return workingDirName;
    }

    JacsServiceData getServiceContext() {
        return serviceContext;
    }

    String getProcessName() {
        return processName;
    }

    int getMaxRunningProcesses() {
        return maxRunningProcesses;
    }

    ExeJobInfo getActualRunningJobInfo() {
        return actualRunningJobInfo;
    }

    void setActualRunningJobInfo(ExeJobInfo actualRunningJobInfo) {
        this.actualRunningJobInfo = actualRunningJobInfo;
    }

    void setJobDoneCallback(JobDoneCallback jobDoneCallback) {
        this.jobDoneCallback = jobDoneCallback;
    }
}

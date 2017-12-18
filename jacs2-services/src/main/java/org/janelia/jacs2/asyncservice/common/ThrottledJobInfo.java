package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsJobInstanceInfo;
import org.janelia.model.service.JacsServiceData;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ThrottledJobInfo implements ExeJobInfo {

    public static interface JobDoneCallback {
        void done(ThrottledJobInfo jobInfo);
    }

    private final ExternalCodeBlock externalCode;
    private final List<ExternalCodeBlock> externalConfigs;
    private final Map<String, String> env;
    private final String scriptDirName;
    private final String processDirName;
    private final JacsServiceData serviceContext;

    private final String processName;
    private final ExternalProcessRunner actualProcessRunner;
    private final int maxRunningProcesses;
    private ExeJobInfo actualRunningJobInfo;
    private volatile boolean terminated;
    private JobDoneCallback jobDoneCallback;

    public ThrottledJobInfo(ExternalCodeBlock externalCode, List<ExternalCodeBlock> externalConfigs, Map<String, String> env,
                            String scriptDirName, String processDirName, JacsServiceData serviceContext,
                            String processName, ExternalProcessRunner actualProcessRunner, int maxRunningProcesses) {
        this.externalCode = externalCode;
        this.externalConfigs = externalConfigs;
        this.env = env;
        this.scriptDirName = scriptDirName;
        this.processDirName = processDirName;
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
    public String start() {
        if (!terminated) {
            ExeJobInfo actualJobInfo = actualProcessRunner.runCmds(getExternalCode(), getExternalConfigs(), getEnv(), getScriptDirName(), getProcessDirName(), getServiceContext());
            setActualRunningJobInfo(actualJobInfo);
            return actualJobInfo.start();
        } else
            return null;
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
            return terminated;
        } else {
            return actualRunningJobInfo.isDone();
        }
    }

    @Override
    public Collection<JacsJobInstanceInfo> getJobInstanceInfos() {
        if (actualRunningJobInfo == null) return Collections.emptyList();
        return actualRunningJobInfo.getJobInstanceInfos();
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
            return terminated;
        } else {
            return actualRunningJobInfo.hasFailed();
        }
    }

    @Override
    public void terminate() {
        if (actualRunningJobInfo != null) {
            actualRunningJobInfo.terminate();
        } else {
            terminated = true;
        }
        if (jobDoneCallback != null) {
            jobDoneCallback.done(this);
        }
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

    String getScriptDirName() {
        return scriptDirName;
    }

    String getProcessDirName() {
        return processDirName;
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

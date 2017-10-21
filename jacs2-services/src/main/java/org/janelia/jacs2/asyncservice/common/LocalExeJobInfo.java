package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.model.jacsservice.JacsJobInstanceInfo;

import java.util.Collection;
import java.util.Collections;

public class LocalExeJobInfo implements ExeJobInfo {
    private final Process localProcess;
    private final String scriptName;
    private boolean done;
    private boolean failed;

    public LocalExeJobInfo(Process localProcess, String scriptName) {
        this.localProcess = localProcess;
        this.scriptName = scriptName;
    }

    @Override
    public String getScriptName() {
        return scriptName;
    }

    @Override
    public boolean isDone() {
        if (done) return done;
        done = !localProcess.isAlive();
        if (done) {
            failed = localProcess.exitValue() != 0;
        }
        return done;
    }

    @Override
    public Collection<JacsJobInstanceInfo> getJobInstanceInfos() {
        return Collections.emptyList();
    }

    @Override
    public boolean hasFailed() {
        return done && failed;
    }

    @Override
    public void terminate() {
        localProcess.destroyForcibly();
    }

}

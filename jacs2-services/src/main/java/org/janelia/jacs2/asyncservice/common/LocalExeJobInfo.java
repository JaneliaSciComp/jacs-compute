package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsJobInstanceInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class LocalExeJobInfo implements ExeJobInfo {
    private final ProcessBuilder localProcessBuilder;
    private final String scriptName;
    private Process localProcess;
    private volatile boolean done;
    private volatile boolean failed;
    private volatile boolean terminated;

    public LocalExeJobInfo(ProcessBuilder localProcessBuilder, String scriptName) {
        this.localProcessBuilder = localProcessBuilder;
        this.scriptName = scriptName;
    }

    @Override
    public String getScriptName() {
        return scriptName;
    }

    @Override
    public String start() {
        if (!terminated) {
            try {
                localProcess = localProcessBuilder.start();
                return localProcess.toString();
            } catch (IOException e) {
                done = true;
                failed = true;
                throw new IllegalStateException(e);
            }
        } else {
            return null;
        }
    }

    @Override
    public boolean isDone() {
        if (done) return done;
        if (localProcess != null) {
            done = !localProcess.isAlive();
            if (done) {
                failed = localProcess.exitValue() != 0;
            }
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
        terminated = true;
        if (localProcess != null) {
            localProcess.destroyForcibly();
        } else {
            done = true;
            failed = true; // consider an error if early terminated;
        }
    }

}

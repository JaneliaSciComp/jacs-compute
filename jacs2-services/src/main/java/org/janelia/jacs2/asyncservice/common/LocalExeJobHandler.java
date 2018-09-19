package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableList;
import org.janelia.model.service.JacsJobInstanceInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

public class LocalExeJobHandler implements ExeJobHandler {
    private final ProcessBuilder localProcessBuilder;
    private final String jobInfo;
    private Process localProcess;
    private JacsJobInstanceInfo localProcessInfo;
    private volatile boolean done;
    private volatile boolean failed;
    private volatile boolean terminated;

    LocalExeJobHandler(String jobInfo, ProcessBuilder localProcessBuilder) {
        this.jobInfo = jobInfo;
        this.localProcessBuilder = localProcessBuilder;
    }

    @Override
    public String getJobInfo() {
        return jobInfo;
    }

    @Override
    public boolean start() {
        if (!terminated) {
            try {
                localProcess = localProcessBuilder.start();
                localProcessInfo = new JacsJobInstanceInfo();
                localProcessInfo.setName(jobInfo);
                localProcessInfo.setStartTime(new Date());
                return true;
            } catch (IOException e) {
                done = true;
                failed = true;
                throw new IllegalStateException(e);
            }
        } else return false;
    }

    @Override
    public boolean isDone() {
        if (done) return done;
        if (localProcess != null) {
            done = !localProcess.isAlive();
            if (done) {
                localProcessInfo.setExitCode(localProcess.exitValue());
                failed = localProcess.exitValue() != 0;
            }
        }
        return done;
    }

    @Override
    public Collection<JacsJobInstanceInfo> getJobInstances() {
        if (localProcessInfo == null) {
            return Collections.emptyList();
        } else {
            return ImmutableList.of(localProcessInfo);
        }
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

package org.janelia.jacs2.asyncservice.common;

import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.Session;

import java.util.ArrayList;
import java.util.List;

public class ArrayDrmaaJobInfo implements ExeJobInfo {
    private final Session drmaaSession;
    private final List<String> waitingJobIds = new ArrayList<>();
    private final List<String> completedJobIds = new ArrayList<>();
    private final String scriptName;
    private boolean done;
    private boolean failed;

    ArrayDrmaaJobInfo(Session drmaaSession, List<String> jobIds, String scriptName) {
        this.drmaaSession = drmaaSession;
        this.waitingJobIds.addAll(jobIds);
        this.scriptName = scriptName;
        this.done = false;
        this.failed = false;
    }

    @Override
    public String getScriptName() {
        return scriptName;
    }

    @Override
    public boolean isDone() {
        if (done) return done;
        boolean allDone = true;
        List<String> recentlyFinishedJobs = new ArrayList<>();
        for (String jobId : waitingJobIds) {
            try {
                int status = drmaaSession.getJobProgramStatus(jobId);
                switch (status) {
                    case Session.UNDETERMINED:
                        break;
                    case Session.QUEUED_ACTIVE:
                        allDone = false;
                        break;
                    case Session.SYSTEM_ON_HOLD:
                    case Session.USER_ON_HOLD:
                    case Session.USER_SYSTEM_ON_HOLD:
                    case Session.RUNNING:
                    case Session.SYSTEM_SUSPENDED:
                    case Session.USER_SUSPENDED:
                    case Session.USER_SYSTEM_SUSPENDED:
                        allDone = false;
                        break;
                    case Session.DONE:
                        recentlyFinishedJobs.add(jobId);
                        break;
                    case Session.FAILED:
                        recentlyFinishedJobs.add(jobId);
                        failed = true;
                        break;
                    default:
                        break;
                }
            } catch (DrmaaException e) {
                throw new IllegalStateException(e);
            }
        }
        completedJobIds.addAll(recentlyFinishedJobs);
        waitingJobIds.removeAll(recentlyFinishedJobs);
        done = allDone;
        return done;
    }

    @Override
    public boolean hasFailed() {
        return done && failed;
    }

    @Override
    public void terminate() {
        if (!done) {
            for (String jobId : waitingJobIds) {
                try {
                    drmaaSession.control(jobId, Session.TERMINATE);
                } catch (DrmaaException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

}

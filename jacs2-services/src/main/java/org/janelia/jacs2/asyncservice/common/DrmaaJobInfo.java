package org.janelia.jacs2.asyncservice.common;

import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.janelia.model.service.JacsJobInstanceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class DrmaaJobInfo implements ExeJobInfo {

    private static final Logger LOG = LoggerFactory.getLogger(DrmaaJobInfo.class);

    private final Session drmaaSession;
    private final String scriptName;
    private final int numJobs;
    private JobTemplate jobTemplate;
    private List<String> runningJobIds = new ArrayList<>();
    private volatile boolean done;
    private volatile boolean failed;
    private volatile boolean terminated;

    DrmaaJobInfo(Session drmaaSession, String scriptName, int numJobs, JobTemplate jobTemplate) {
        this.drmaaSession = drmaaSession;
        this.scriptName = scriptName;
        this.numJobs = numJobs;
        this.jobTemplate = jobTemplate;
        this.done = false;
        this.failed = false;
    }

    @Override
    public String getScriptName() {
        return scriptName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public String start() {
        if (!terminated) {
            try {
                if (numJobs <= 1) {
                    runningJobIds.add(drmaaSession.runJob(jobTemplate));
                } else {
                    runningJobIds.addAll((List<String>) drmaaSession.runBulkJobs(jobTemplate, 1, numJobs, 1));
                }
                LOG.info("Submitted job {}", runningJobIds);
                return runningJobIds
                        .stream()
                        .reduce((j1, j2) -> j1 + "," + j2)
                        .orElse(null)
                        ;
            } catch (Exception e) {
                done = true;
                failed = true;
                LOG.error("Error starting job {}", scriptName, e);
                throw new IllegalStateException(e);
            } finally {
                cleanTemplate();
            }
        } else {
            return null;
        }
    }

    @Override
    public synchronized boolean isDone() {
        if (done) return done;
        List<String> completedJobs = new ArrayList<>();
        for (String jobId : runningJobIds) {
            if (isDone(jobId)) {
                completedJobs.add(jobId);
            }
        }
        runningJobIds.removeAll(completedJobs);
        if (runningJobIds.isEmpty()) {
            done = true;
        }
        return done;
    }

    private void cleanTemplate() {
        if (jobTemplate != null) {
            try {
                drmaaSession.deleteJobTemplate(jobTemplate);
            } catch (Exception e) {
                LOG.error("Error cleaning drmaa job template for {}", scriptName, e);
            } finally {
                jobTemplate = null;
            }
        }
    }

    private boolean isDone(String jobId) {
        try {
            int status = drmaaSession.getJobProgramStatus(jobId);
            switch (status) {
                case Session.QUEUED_ACTIVE:
                case Session.SYSTEM_ON_HOLD:
                case Session.USER_ON_HOLD:
                case Session.USER_SYSTEM_ON_HOLD:
                case Session.RUNNING:
                case Session.SYSTEM_SUSPENDED:
                case Session.USER_SUSPENDED:
                case Session.USER_SYSTEM_SUSPENDED:
                    return false;
                case Session.DONE:
                    LOG.info("Job {} completed", jobId);
                    return true;
                case Session.UNDETERMINED:
                case Session.FAILED:
                default:
                    done = true;
                    failed = true;
                    return true;
            }
        } catch (DrmaaException e) {
            throw new IllegalStateException(e);
        }
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
        if (!done) {
            try {
                for (String jobId : runningJobIds) {
                    drmaaSession.control(jobId, Session.TERMINATE);
                }
            } catch (DrmaaException e) {
                throw new IllegalStateException(e);
            }
        }
    }

}

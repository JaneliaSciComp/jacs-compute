package org.janelia.jacs2.asyncservice.common;

import org.ggf.drmaa.DrmaaException;
import org.ggf.drmaa.JobTemplate;
import org.ggf.drmaa.Session;
import org.janelia.model.service.JacsJobInstanceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DrmaaJobHandler implements JobHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DrmaaJobHandler.class);

    private final String jobInfo;
    private final Session drmaaSession;
    private final int numJobs;
    private JobTemplate jobTemplate;
    private Map<String, JacsJobInstanceInfo> jobInstances = new LinkedHashMap<>();
    private volatile boolean done;
    private volatile boolean failed;
    private volatile boolean terminated;

    DrmaaJobHandler(String jobInfo, Session drmaaSession, int numJobs, JobTemplate jobTemplate) {
        this.drmaaSession = drmaaSession;
        this.jobInfo = jobInfo;
        this.numJobs = numJobs;
        this.jobTemplate = jobTemplate;
        this.done = false;
        this.failed = false;
    }

    @Override
    public String getJobInfo() {
        return jobInfo + '[' +
                jobInstances.entrySet()
                        .stream()
                        .map(je -> je.getKey())
                        .reduce((j1, j2) -> j1 + "," + j2)
                        .orElse("") +
                ']';
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean start() {
        if (!terminated) {
            try {
                if (numJobs <= 1) {
                    String jobId = drmaaSession.runJob(jobTemplate);
                    jobInstances.put(jobId, createJobInstance(jobId, new Date()));
                    LOG.info("Submitted job {}", jobInstances.keySet());
                } else {
                    Date startTime = new Date();
                    List<String> jobIds = (List<String>) drmaaSession.runBulkJobs(jobTemplate, 1, numJobs, 1);
                    jobIds.forEach(jobId -> jobInstances.put(jobId, createJobInstance(jobId, startTime)));
                    LOG.info("Submitted jobs {}", jobIds);
                }
                return true;
            } catch (Exception e) {
                done = true;
                failed = true;
                LOG.error("Error starting job {}", jobInfo, e);
                throw new IllegalStateException(e);
            } finally {
                cleanTemplate();
            }
        } else {
            return false;
        }
    }

    private JacsJobInstanceInfo createJobInstance(String jobId, Date startDate) {
        JacsJobInstanceInfo jobInstanceInfo = new JacsJobInstanceInfo();
        jobInstanceInfo.setName(jobId);
        jobInstanceInfo.setStartTime(startDate);
        return jobInstanceInfo;
    }
    @Override
    public synchronized boolean isDone() {
        if (done) return done;
        done = jobInstances.entrySet()
                .stream()
                .filter(jobInstanceInfoEntry -> jobInstanceInfoEntry.getValue().getFinishTime() == null)
                .filter(jobInstanceInfoEntry -> !isDone(jobInstanceInfoEntry.getKey(), jobInstanceInfoEntry.getValue()))
                .findAny()
                .map(jobInstanceInfoEntry -> true)
                .orElse(true);
        return done;
    }

    private void cleanTemplate() {
        if (jobTemplate != null) {
            try {
                drmaaSession.deleteJobTemplate(jobTemplate);
            } catch (Exception e) {
                LOG.error("Error cleaning drmaa job template for {}", getJobInfo(), e);
            } finally {
                jobTemplate = null;
            }
        }
    }

    private boolean isDone(String jobId, JacsJobInstanceInfo jobInstanceInfo) {
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
                    jobInstanceInfo.setFinishTime(new Date());
                    return true;
                case Session.UNDETERMINED:
                case Session.FAILED:
                default:
                    done = true;
                    failed = true;
                    jobInstanceInfo.setFinishTime(new Date());
                    return true;
            }
        } catch (DrmaaException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Collection<JacsJobInstanceInfo> getJobInstances() {
        return jobInstances.values();
    }

    @Override
    public boolean hasFailed() {
        return done && failed;
    }

    @Override
    public void terminate() {
        terminated = true;
        if (!done) {
            jobInstances.entrySet()
                    .stream()
                    .filter(jobInstanceInfoEntry -> jobInstanceInfoEntry.getValue().getFinishTime() == null)
                    .forEach(jobInstanceInfoEntry -> {
                        try {
                            drmaaSession.control(jobInstanceInfoEntry.getKey(), Session.TERMINATE);
                        } catch (DrmaaException e) {
                            LOG.error("Error terminating {}", jobInstanceInfoEntry.getKey(), e);
                        }
                    });
        }
    }

}

package org.janelia.jacs2.asyncservice.common.cluster;

import org.janelia.cluster.JobFuture;
import org.janelia.cluster.JobInfo;
import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobMetadata;
import org.janelia.cluster.JobStatus;
import org.janelia.cluster.JobTemplate;
import org.janelia.jacs2.asyncservice.common.ExeJobHandler;
import org.janelia.model.service.JacsJobInstanceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Handler for a LSF job.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 * @see org.janelia.jacs2.asyncservice.qualifier.LSFJavaJob
 */
public class LsfJavaExeJobHandler implements ExeJobHandler {

    private static final Logger LOG = LoggerFactory.getLogger(LsfJavaExeJobHandler.class);

    private final String jobInfo;
    private final JobManager jobMgr;
    private final JobTemplate jobTemplate;
    private final int numJobs;
    private Long jobId;
    private volatile boolean done;
    private volatile boolean failed;
    private volatile boolean terminated;

    public LsfJavaExeJobHandler(String jobInfo, JobManager jobMgr, JobTemplate jobTemplate, int numJobs) {
        this.jobInfo = jobInfo;
        this.jobMgr = jobMgr;
        this.jobTemplate = jobTemplate;
        this.numJobs = numJobs;
    }

    @Override
    public String getJobInfo() {
        StringBuilder jobInfoBuilder = new StringBuilder(jobInfo);
        if (jobId != null) {
            jobInfoBuilder.append('[').append(jobId).append(']');
        }
        return jobInfoBuilder.toString();
    }

    @Override
    public boolean start() {
        if (!terminated) {
            try {
                JobFuture jobFuture = jobMgr.submitJob(jobTemplate, 1, numJobs);
                jobId = jobFuture.getJobId();
                LOG.info("Submitted job {}", jobId);
                return true;
            } catch (Exception e) {
                LOG.error("Error submitting {} of {} with {}", numJobs, jobInfo, jobTemplate, e);
                done = true;
                failed = true;
                throw new IllegalStateException(e);
            }
        } else {
            return false;
        }
    }

    @Override
    public boolean isDone() {
        if (jobId == null) {
            return false;
        }
        JobMetadata jobMetadata = jobMgr.getJobMetadata(jobId);

        if (jobMetadata != null) {
            // Check to see if any job in the job array failed
            for (JobInfo jobInfo : jobMetadata.getLastInfos()) {
                if (jobInfo.getStatus() == JobStatus.EXIT
                        || (jobInfo.getExitCode() != null && jobInfo.getExitCode() != 0)) {
                    this.failed = true;
                    break;
                }
            }
            done = jobMetadata.isDone();
            if (done && LOG.isInfoEnabled()) {
                logJobInfo();
            }
        } else {
            // If there is no information about a job, assume it is done
            this.done = true;
            this.failed = true;
            LOG.error("No job information found for {}. Assuming that it failed.", jobId);
        }

        return done;
    }

    private void logJobInfo() {
        for (JacsJobInstanceInfo jobInfo : getJobInstances()) {
            Long queueTimeSeconds = jobInfo.getQueueSecs();
            Long runTimeSeconds = jobInfo.getRunSecs();

            String queueTime = queueTimeSeconds+" sec";
            if (queueTimeSeconds!=null && queueTimeSeconds>300) { // More than 5 minutes, just show the minutes
                queueTime = TimeUnit.MINUTES.convert(queueTimeSeconds, TimeUnit.SECONDS) + " min";
            }

            String runTime = runTimeSeconds+" sec";
            if (runTimeSeconds!=null && runTimeSeconds>300) {
                runTime = TimeUnit.MINUTES.convert(runTimeSeconds, TimeUnit.SECONDS) + " min";
            }

            String maxMem = jobInfo.getMaxMem();
            String jobIdStr = jobInfo.getJobId()+"";
            if (jobInfo.getArrayIndex()!=null) {
                jobIdStr += "."+jobInfo.getArrayIndex();
            }

            LOG.info("Job {} was queued for {}, ran for {}, and used "+maxMem+" of memory.", jobIdStr, queueTime, runTime);
            if (jobInfo.getExitCode() != 0) {
                LOG.error("Job {} exited with code {} and reason {}", jobIdStr, jobInfo.getExitCode(), jobInfo.getExitReason());
            }
        }
    }

    @Override
    public Collection<JacsJobInstanceInfo> getJobInstances() {
        if (jobId == null) {
            return Collections.emptyList();
        }
        Collection<JacsJobInstanceInfo> infos = new ArrayList<>();
        JobMetadata jobMetadata = jobMgr.getJobMetadata(jobId);
        if (jobMetadata != null) {
            for (JobInfo jobInfo : jobMetadata.getLastInfos()) {
                JacsJobInstanceInfo jacsJobInstanceInfo = new JacsJobInstanceInfo();
                jacsJobInstanceInfo.setJobId(jobInfo.getJobId());
                jacsJobInstanceInfo.setArrayIndex(jobInfo.getArrayIndex());
                jacsJobInstanceInfo.setName(jobInfo.getName());
                jacsJobInstanceInfo.setFromHost(jobInfo.getFromHost());
                jacsJobInstanceInfo.setExecHost(jobInfo.getExecHost());
                jacsJobInstanceInfo.setStatus(jobInfo.getStatus() == null ? null : jobInfo.getStatus().name());
                jacsJobInstanceInfo.setQueue(jobInfo.getQueue());
                jacsJobInstanceInfo.setProject(jobInfo.getProject());
                jacsJobInstanceInfo.setReqSlot(jobInfo.getReqSlot());
                jacsJobInstanceInfo.setAllocSlot(jobInfo.getAllocSlot());
                jacsJobInstanceInfo.setSubmitTime(LsfParseUtils.convertLocalDateTime(jobInfo.getSubmitTime()));
                jacsJobInstanceInfo.setStartTime(LsfParseUtils.convertLocalDateTime(jobInfo.getStartTime()));
                jacsJobInstanceInfo.setFinishTime(LsfParseUtils.convertLocalDateTime(jobInfo.getFinishTime()));
                jacsJobInstanceInfo.setQueueSecs(LsfParseUtils.getDiffSecs(jobInfo.getSubmitTime(), jobInfo.getStartTime()));
                jacsJobInstanceInfo.setRunSecs(LsfParseUtils.getDiffSecs(jobInfo.getStartTime(), jobInfo.getFinishTime()));
                jacsJobInstanceInfo.setMaxMem(jobInfo.getMaxMem());
                jacsJobInstanceInfo.setMaxMemBytes(LsfParseUtils.parseMemToBytes(jobInfo.getMaxMem()));
                jacsJobInstanceInfo.setExitCode(jobInfo.getExitCode());
                jacsJobInstanceInfo.setExitReason(jobInfo.getExitReason());
                infos.add(jacsJobInstanceInfo);
            }
        }
        return infos;
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
                jobMgr.killJob(jobId);
            } catch (Exception e) {
                LOG.warn("Error while terminating job {}", jobId, e);
            } finally {
                done = true;
            }
        }
    }

}

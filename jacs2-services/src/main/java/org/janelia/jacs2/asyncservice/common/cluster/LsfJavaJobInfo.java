package org.janelia.jacs2.asyncservice.common.cluster;

import org.janelia.cluster.JobInfo;
import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobMetadata;
import org.janelia.cluster.JobStatus;
import org.janelia.jacs2.asyncservice.common.ExeJobInfo;
import org.janelia.model.service.JacsJobInstanceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Job info for a job running using the LSFJavaJob runner.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 * @see org.janelia.jacs2.asyncservice.qualifier.LSFJavaJob
 */
public class LsfJavaJobInfo implements ExeJobInfo {

    private static final Logger log = LoggerFactory.getLogger(LsfJavaJobInfo.class);

    private final JobManager jobMgr;
    private final Long jobId;
    private final String scriptName;
    private boolean done = false;
    private boolean failed = false;

    public LsfJavaJobInfo(JobManager jobMgr, Long jobId, String scriptName) {
        this.jobMgr = jobMgr;
        this.jobId = jobId;
        this.scriptName = scriptName;
    }

    @Override
    public String getScriptName() {
        return scriptName;
    }

    @Override
    public boolean isDone() {
        if (done) return done;

        JobMetadata jobMetadata = jobMgr.getJobMetadata(jobId);

        if (jobMetadata!=null) {
            // Check to see if any job in the job array failed
            for (JobInfo jobInfo : jobMetadata.getLastInfos()) {
                if (jobInfo.getStatus() == JobStatus.EXIT
                        || (jobInfo.getExitCode() != null && jobInfo.getExitCode() != 0)) {
                    this.failed = true;
                    break;
                }
            }

            this.done = jobMetadata.isDone();
        }
        else {
            // If there is no information about a job, assume it is done
            this.done = true;
            this.failed = true;
            log.error("No job information found for {}. Assuming that it failed.", jobId);
        }

        return done;
    }

    @Override
    public Collection<JacsJobInstanceInfo> getJobInstanceInfos() {
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
        if (!done) {
            // TODO: if we need this, first we'll need to implement bkill in java-lsf library
            throw new UnsupportedOperationException();
        }
    }

}

package org.janelia.jacs2.asyncservice.common.cluster;

import org.janelia.cluster.JobInfo;
import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobMetadata;
import org.janelia.cluster.JobStatus;
import org.janelia.jacs2.asyncservice.common.ExeJobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

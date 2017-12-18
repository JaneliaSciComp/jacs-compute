package org.janelia.jacs2.asyncservice.common.cluster;

import org.janelia.cluster.JobFuture;
import org.janelia.cluster.JobInfo;
import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobMetadata;
import org.janelia.cluster.JobStatus;
import org.janelia.cluster.JobTemplate;
import org.janelia.jacs2.asyncservice.common.ExeJobInfo;
import org.janelia.model.service.JacsJobInstanceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Job info for a job running using the LSFJavaJob runner.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 * @see org.janelia.jacs2.asyncservice.qualifier.LSFJavaJob
 */
public class LsfJavaJobInfo implements ExeJobInfo {

    private static final Logger LOG = LoggerFactory.getLogger(LsfJavaJobInfo.class);

    private final JobManager jobMgr;
    private final JobTemplate jobTemplate;
    private final int numJobs;
    private final String scriptName;
    private final ExecutorService executorService;
    private JobFuture jobFuture;
    private boolean done = false;
    private boolean failed = false;

    public LsfJavaJobInfo(JobManager jobMgr, JobTemplate jobTemplate, int numJobs, String scriptName, ExecutorService executorService) {
        this.jobMgr = jobMgr;
        this.jobTemplate = jobTemplate;
        this.numJobs = numJobs;
        this.scriptName = scriptName;
        this.executorService = executorService;
    }

    @Override
    public String getScriptName() {
        return scriptName;
    }

    @Override
    public String start() {
        try {
            jobFuture = jobMgr.submitJob(jobTemplate, 1, numJobs);
        } catch (Exception e) {
            done = true;
            failed = true;
            throw new IllegalStateException(e);
        }
        jobFuture.whenCompleteAsync((Collection<JobInfo> jobInfoList, Throwable e) -> {
            if (e != null) {
                failed = true;
                LOG.error("There was a problem during execution on LSF", e);
            } else if (LOG.isInfoEnabled()) {
                logJobInfo();
            }
        }, executorService);
        return jobFuture.getJobId().toString();
    }

    private void logJobInfo() {
        for (JacsJobInstanceInfo jobInfo : getJobInstanceInfos()) {
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
            if (jobInfo.getExitCode()!=0) {
                LOG.error("Job {} exited with code {} and reason {}", jobIdStr, jobInfo.getExitCode(), jobInfo.getExitReason());
            }
        }
    }

    @Override
    public boolean isDone() {
        if (done) return true;
        if (jobFuture == null) {
            return false;
        }
        JobMetadata jobMetadata = jobMgr.getJobMetadata(jobFuture.getJobId());

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
        } else {
            // If there is no information about a job, assume it is done
            this.done = true;
            this.failed = true;
            LOG.error("No job information found for {}. Assuming that it failed.", jobFuture.getJobId());
        }

        return done;
    }

    @Override
    public Collection<JacsJobInstanceInfo> getJobInstanceInfos() {
        Collection<JacsJobInstanceInfo> infos = new ArrayList<>();
        if (jobFuture == null) {
            return infos;
        }
        JobMetadata jobMetadata = jobMgr.getJobMetadata(jobFuture.getJobId());
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

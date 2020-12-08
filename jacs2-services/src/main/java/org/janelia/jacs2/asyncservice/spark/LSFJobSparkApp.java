package org.janelia.jacs2.asyncservice.spark;

import java.util.Collection;

import org.janelia.cluster.JobInfo;
import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LSFJobSparkApp extends AbstractSparkApp {
    private static final Logger LOG = LoggerFactory.getLogger(LSFJobSparkApp.class);

    private final JobManager jobManager;
    private final Long driverJobId;

    public LSFJobSparkApp(JobManager jobManager, Long driverJobId, String errorFilename) {
        super(errorFilename);
        this.jobManager = jobManager;
        this.driverJobId = driverJobId;
    }

    @Override
    public String getAppId() {
        return driverJobId != null ? driverJobId.toString() : null;
    }

    @Override
    public boolean isDone() {
        if (driverJobId == null) {
            LOG.warn("!!!! ISDONE: Driver job id was not set");
            return true;
        } else {
            return jobManager.retrieveJobInfo(driverJobId).stream().allMatch(JobInfo::isComplete);
        }
    }

    @Override
    public String getStatus() {
        if (driverJobId == null) {
            LOG.warn("!!!! GET_STATUS: Driver job id was not set");
            return "NOT_FOUND";
        } else {
            Collection<JobInfo> jobInfoCollection = jobManager.retrieveJobInfo(driverJobId);
            LOG.info("!!!! JOB INFO {}", jobInfoCollection);
            return jobInfoCollection.stream()
                    .map(ji -> ji.getStatus().name())
                    .findFirst()
                    .orElse("NOT_FOUND");
        }
    }

    @Override
    public String getErrors() {
        if (completedWithErrors()) {
            return "Spark Application terminated with errors";
        } else {
            return super.getErrors();
        }
    }

    private boolean completedWithErrors() {
        if (driverJobId == null) {
            return true; // if it was never started consider it an error
        } else {
            return jobManager.retrieveJobInfo(driverJobId).stream()
                    .filter(jobInfo -> jobInfo.getStatus() == JobStatus.EXIT)
                    .count() > 0;
        }
    }

    @Override
    public void kill() {
        if (driverJobId != null) {
            try {
                jobManager.killJob(driverJobId);
            } catch (Exception ignore) {
            }
        }
    }
}

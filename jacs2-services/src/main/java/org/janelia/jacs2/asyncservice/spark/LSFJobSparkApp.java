package org.janelia.jacs2.asyncservice.spark;

import org.janelia.cluster.JobInfo;
import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobStatus;

public class LSFJobSparkApp extends AbstractSparkApp {
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
            return true;
        } else {
            return jobManager.getJobInfo(driverJobId).stream()
                .filter(JobInfo::isComplete)
                .count() == 0;
        }
    }

    @Override
    public String getStatus() {
        if (driverJobId == null) {
            return "NOT_FOUND";
        } else {
            return jobManager.getJobInfo(driverJobId).stream()
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
            return jobManager.getJobInfo(driverJobId).stream()
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

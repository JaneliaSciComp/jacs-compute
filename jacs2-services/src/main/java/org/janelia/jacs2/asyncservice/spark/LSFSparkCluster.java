package org.janelia.jacs2.asyncservice.spark;

import javax.annotation.Nonnull;

import org.janelia.cluster.JobManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LSFSparkCluster {
    private static final Logger LOG = LoggerFactory.getLogger(LSFSparkCluster.class);

    private final JobManager jobMgr;
    private final SparkClusterInfo sparkClusterInfo;

    LSFSparkCluster(JobManager jobMgr, @Nonnull SparkClusterInfo sparkClusterInfo) {
        this.jobMgr = jobMgr;
        this.sparkClusterInfo = sparkClusterInfo;
    }

    SparkClusterInfo getSparkClusterInfo() {
        return sparkClusterInfo;
    }

    void stopCluster() {
        try {
            LOG.info("Kill spark master job {}", sparkClusterInfo.getMasterJobId());
            jobMgr.killJob(sparkClusterInfo.getMasterJobId());
        } catch (Exception e) {
            LOG.error("Error stopping spark master job {}", sparkClusterInfo.getMasterJobId(), e);
        }
        try {
            LOG.info("Kill spark worker job {}", sparkClusterInfo.getWorkerJobId());
            jobMgr.killJob(sparkClusterInfo.getWorkerJobId());
        } catch (Exception e) {
            LOG.error("Error stopping spark worker job {}", sparkClusterInfo.getWorkerJobId(), e);
        }
    }
}

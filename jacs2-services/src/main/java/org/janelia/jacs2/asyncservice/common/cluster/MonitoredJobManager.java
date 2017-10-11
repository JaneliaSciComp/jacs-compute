package org.janelia.jacs2.asyncservice.common.cluster;

import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobMonitor;
import org.janelia.cluster.lsf.LsfSyncApi;
import org.janelia.jacs2.cdi.LsfProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.Vetoed;

/**
 * Attaches a monitoring thread to a JobManager.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Vetoed // Should be created by LsfProducer
public class MonitoredJobManager {

    private static final Logger log = LoggerFactory.getLogger(LsfProducer.class);

    private JobManager jobMgr;
    private JobMonitor monitor;

    public MonitoredJobManager() {
        this.jobMgr = new JobManager(new LsfSyncApi());
        this.monitor = new JobMonitor(jobMgr);
        log.info("Starting job manager");
        monitor.start();
    }

    public JobManager getJobMgr() {
        return jobMgr;
    }

    public void start() {
        monitor.start();
    }

    public void stop() {
        monitor.stop();
    }
}

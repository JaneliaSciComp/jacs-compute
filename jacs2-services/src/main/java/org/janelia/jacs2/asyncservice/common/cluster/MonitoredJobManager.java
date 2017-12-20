package org.janelia.jacs2.asyncservice.common.cluster;

import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobMonitor;
import org.janelia.cluster.lsf.LsfSyncApi;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Attaches a monitoring thread to a JobManager.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Singleton
public class MonitoredJobManager {

    private static final Logger log = LoggerFactory.getLogger(MonitoredJobManager.class);

    private JobManager jobMgr;
    private JobMonitor monitor;

    @Inject
    public MonitoredJobManager(
            @PropertyValue(name = "service.cluster.checkIntervalInSeconds") int checkIntervalInSeconds) {
        log.info("Creating monitored job manager");
        this.jobMgr = new JobManager(new LsfSyncApi());
        this.monitor = new JobMonitor(jobMgr, checkIntervalInSeconds);
    }

    @PostConstruct
    private void init() {
        log.info("Starting job manager monitoring");
        monitor.start();
    }

    @PreDestroy
    private void destroy() {
        log.info("Stopping job manager monitoring");
        monitor.stop();
    }

    public JobManager getJobMgr() {
        return jobMgr;
    }
}

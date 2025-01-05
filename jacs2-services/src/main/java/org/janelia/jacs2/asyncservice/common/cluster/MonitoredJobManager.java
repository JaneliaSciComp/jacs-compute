package org.janelia.jacs2.asyncservice.common.cluster;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.janelia.cluster.JobManager;
import org.janelia.cluster.JobMonitor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Attaches a monitoring thread to a JobManager.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Dependent
public class MonitoredJobManager {

    private static final Logger LOG = LoggerFactory.getLogger(MonitoredJobManager.class);

    private final JobManager jobMgr;
    private final JobMonitor monitor;

    @Inject
    public MonitoredJobManager(JobManagerProvider jobMgrProvider,
                               @PropertyValue(name = "service.cluster.checkIntervalInSeconds") int checkIntervalInSeconds) {
        LOG.info("Creating monitored job manager");
        this.jobMgr = jobMgrProvider.get();
        this.monitor = new JobMonitor(jobMgr, checkIntervalInSeconds, false);
    }

    @PostConstruct
    private void init() {
        LOG.info("Starting job manager monitoring");
        monitor.start();
    }

    @PreDestroy
    private void destroy() {
        LOG.info("Stopping job manager monitoring");
        monitor.stop();
    }

    public JobManager getJobMgr() {
        return jobMgr;
    }
}

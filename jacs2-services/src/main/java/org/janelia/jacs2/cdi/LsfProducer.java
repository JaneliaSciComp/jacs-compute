package org.janelia.jacs2.cdi;

import org.ggf.drmaa.DrmaaException;
import org.janelia.jacs2.asyncservice.common.cluster.MonitoredJobManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

@ApplicationScoped
public class LsfProducer {

    private static final Logger log = LoggerFactory.getLogger(LsfProducer.class);

    @ApplicationScoped
    @Produces
    public MonitoredJobManager createJobManager() throws DrmaaException {
        MonitoredJobManager jobMgr = new MonitoredJobManager();
        log.info("Starting job manager");
        jobMgr.start();
        return jobMgr;
    }

    public void closeJobManager(@Disposes @Default MonitoredJobManager jobMgr) throws DrmaaException {
        log.info("Closing job manager");
        jobMgr.stop();
    }
}

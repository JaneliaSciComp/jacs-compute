package org.janelia.jacs2.job;

import org.janelia.jacs2.asyncservice.common.JacsJobRunner;
import org.janelia.jacs2.asyncservice.common.JacsQueueSyncer;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationQueue;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.CDI;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

@ApplicationScoped
public class BackgroundJobs implements ServletContextListener {

    private JacsQueueSyncer queueSyncer;
    private JacsJobRunner jobRunner;
    private ServiceComputationQueue taskQueuePoller;

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        Instance<JacsQueueSyncer> queueSyncerSource = CDI.current().select(JacsQueueSyncer.class);
        Instance<JacsJobRunner> jobRunnerSource = CDI.current().select(JacsJobRunner.class);
        Instance<ServiceComputationQueue> taskQueuePollerSource = CDI.current().select(ServiceComputationQueue.class);
        queueSyncer = queueSyncerSource.get();
        jobRunner = jobRunnerSource.get();
        taskQueuePoller = taskQueuePollerSource.get();
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        queueSyncer.destroy();
        jobRunner.destroy();
        taskQueuePoller.destroy();
    }

}

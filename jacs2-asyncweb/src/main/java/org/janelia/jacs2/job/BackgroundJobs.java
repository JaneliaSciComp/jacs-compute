package org.janelia.jacs2.job;

import org.janelia.jacs2.asyncservice.common.JacsScheduledServiceRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceDispatchRunner;
import org.janelia.jacs2.asyncservice.common.JacsQueueSyncer;
import org.janelia.jacs2.asyncservice.common.ServiceComputationQueue;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.spi.CDI;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

@ApplicationScoped
public class BackgroundJobs implements ServletContextListener {

    private JacsQueueSyncer queueSyncer;
    private JacsServiceDispatchRunner serviceDispatchRunner;
    private JacsScheduledServiceRunner scheduledServicesRunner;
    private ServiceComputationQueue taskQueuePoller;

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        Instance<JacsQueueSyncer> queueSyncerSource = CDI.current().select(JacsQueueSyncer.class);
        Instance<JacsServiceDispatchRunner> serviceDispatchRunnerSource = CDI.current().select(JacsServiceDispatchRunner.class);
        Instance<JacsScheduledServiceRunner> scheduledServicesRunnerSource = CDI.current().select(JacsScheduledServiceRunner.class);
        Instance<ServiceComputationQueue> taskQueuePollerSource = CDI.current().select(ServiceComputationQueue.class);
        queueSyncer = queueSyncerSource.get();
        serviceDispatchRunner = serviceDispatchRunnerSource.get();
        scheduledServicesRunner = scheduledServicesRunnerSource.get();
        taskQueuePoller = taskQueuePollerSource.get();
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        queueSyncer.destroy();
        serviceDispatchRunner.destroy();
        scheduledServicesRunner.destroy();
        taskQueuePoller.destroy();
    }

}

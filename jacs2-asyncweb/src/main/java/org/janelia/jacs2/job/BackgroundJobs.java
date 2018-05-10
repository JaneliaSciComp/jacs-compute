package org.janelia.jacs2.job;

import org.janelia.jacs2.asyncservice.common.JacsQueueSyncer;
import org.janelia.jacs2.asyncservice.common.JacsScheduledServiceRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceDispatchRunner;
import org.janelia.jacs2.asyncservice.common.ServiceComputationQueue;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
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
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit.initialize();
        queueSyncer = container.select(JacsQueueSyncer.class).get();
        serviceDispatchRunner = container.select(JacsServiceDispatchRunner.class).get();
        scheduledServicesRunner = container.select(JacsScheduledServiceRunner.class).get();
        taskQueuePoller = container.select(ServiceComputationQueue.class).get();
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        queueSyncer.destroy();
        serviceDispatchRunner.destroy();
        scheduledServicesRunner.destroy();
        taskQueuePoller.destroy();
    }

}

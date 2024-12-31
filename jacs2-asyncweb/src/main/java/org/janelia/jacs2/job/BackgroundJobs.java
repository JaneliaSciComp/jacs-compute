package org.janelia.jacs2.job;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;

import org.janelia.jacs2.asyncservice.common.JacsQueueSyncer;
import org.janelia.jacs2.asyncservice.common.JacsScheduledServiceRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceDispatchRunner;
import org.janelia.jacs2.asyncservice.common.ServiceComputationQueue;

public class BackgroundJobs implements ServletContextListener {

    private JacsQueueSyncer queueSyncer;
    private JacsServiceDispatchRunner serviceDispatchRunner;
    private JacsScheduledServiceRunner scheduledServicesRunner;
    private ServiceComputationQueue taskQueuePoller;

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        SeContainerInitializer containerInitializer = SeContainerInitializer.newInstance();
        SeContainer seContainer = containerInitializer.initialize();
        queueSyncer = seContainer.select(JacsQueueSyncer.class).get();
        serviceDispatchRunner = seContainer.select(JacsServiceDispatchRunner.class).get();
        scheduledServicesRunner = seContainer.select(JacsScheduledServiceRunner.class).get();
        taskQueuePoller = seContainer.select(ServiceComputationQueue.class).get();
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        queueSyncer.destroy();
        serviceDispatchRunner.destroy();
        scheduledServicesRunner.destroy();
        taskQueuePoller.destroy();
    }

}

package org.janelia.jacs2.job;

import org.janelia.jacs2.asyncservice.common.JacsQueueSyncer;
import org.janelia.jacs2.asyncservice.common.JacsScheduledServiceRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceDispatchRunner;
import org.janelia.jacs2.asyncservice.common.ServiceComputationQueue;
import org.janelia.jacs2.cdi.SeContainerFactory;

import javax.enterprise.inject.se.SeContainer;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class BackgroundJobs implements ServletContextListener {

    private JacsQueueSyncer queueSyncer;
    private JacsServiceDispatchRunner serviceDispatchRunner;
    private JacsScheduledServiceRunner scheduledServicesRunner;
    private ServiceComputationQueue taskQueuePoller;

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        SeContainer seContainer = SeContainerFactory.getSeContainer();
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

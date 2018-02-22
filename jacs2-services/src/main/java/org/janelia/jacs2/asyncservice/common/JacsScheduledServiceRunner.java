package org.janelia.jacs2.asyncservice.common;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.cronservice.CronScheduledServiceManager;
import org.janelia.model.service.JacsScheduledServiceData;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class JacsScheduledServiceRunner {

    private CronScheduledServiceManager jacsScheduledServiceDataManager;
    private ScheduledExecutorService scheduler;
    private Logger logger;
    private int initialDelay;
    private int period;

    @Inject
    public JacsScheduledServiceRunner(CronScheduledServiceManager jacsScheduledServiceDataManager,
                                      @IntPropertyValue(name = "service.crontab.InitialDelayInSeconds", defaultValue = 30) int initialDelay,
                                      @IntPropertyValue(name = "service.crontab.PeriodInSeconds", defaultValue = 60) int period,
                                      Logger logger) {
        this.jacsScheduledServiceDataManager = jacsScheduledServiceDataManager;
        this.initialDelay = initialDelay == 0 ? 30 : initialDelay;
        this.period = period == 0 ? 10 : period;
        this.logger = logger;
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("JACS-CRONTAB-%d")
                .setDaemon(true)
                .build();
        this.scheduler = Executors.newScheduledThreadPool(1, threadFactory);
    }

    private void doWork() {
        try {
            List<JacsServiceData> scheduledServices = jacsScheduledServiceDataManager.scheduleServices();
            if (scheduledServices.size() > 0) {
                logger.info("Service scheduler created {}", scheduledServices);
            }
        } catch (Exception e) {
            logger.error("Critical error - job scheduler failed", e);
        }
    }

    @PostConstruct
    public void initialize() {
        scheduler.scheduleAtFixedRate(() -> doWork(), initialDelay, period, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void destroy() {
        scheduler.shutdownNow();
    }
}

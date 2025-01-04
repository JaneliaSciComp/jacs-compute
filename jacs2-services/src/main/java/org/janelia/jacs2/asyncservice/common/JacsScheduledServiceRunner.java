package org.janelia.jacs2.asyncservice.common;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.dataservice.cronservice.CronScheduledServiceManager;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

@Dependent
public class JacsScheduledServiceRunner {

    private static final int DEFAULT_INITIAL_DELAY = 60;
    private static final int DEFAULT_PERIOD = 60;

    private CronScheduledServiceManager jacsScheduledServiceDataManager;
    private ScheduledExecutorService scheduler;
    private Logger logger;
    private int initialDelay;
    private int period;

    @Inject
    public JacsScheduledServiceRunner(CronScheduledServiceManager jacsScheduledServiceDataManager,
                                      @IntPropertyValue(name = "service.crontab.InitialDelayInSeconds", defaultValue = DEFAULT_INITIAL_DELAY) int initialDelay,
                                      @IntPropertyValue(name = "service.crontab.PeriodInSeconds", defaultValue = DEFAULT_PERIOD) int period,
                                      Logger logger) {
        this.jacsScheduledServiceDataManager = jacsScheduledServiceDataManager;
        this.initialDelay = initialDelay == 0 ? DEFAULT_INITIAL_DELAY : initialDelay;
        this.period = period == 0 ? DEFAULT_PERIOD : period;
        this.logger = logger;
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("JACS-CRONTAB-%d")
                .setDaemon(true)
                .build();
        this.scheduler = Executors.newScheduledThreadPool(1, threadFactory);
    }

    private void doWork() {
        try {
            List<JacsServiceData> scheduledServices = jacsScheduledServiceDataManager.scheduleServices(Duration.ofSeconds(period));
            if (scheduledServices.size() > 0) {
                logger.info("Service scheduler created {}", scheduledServices);
            }
        } catch (Exception e) {
            logger.error("Critical error - service scheduler failed", e);
        }
    }

    @PostConstruct
    public void initialize() {
        logger.info("Initialize scheduled jobs executor to run every {}s with an initial delay of {}s", period, initialDelay);
        scheduler.scheduleAtFixedRate(() -> doWork(), initialDelay, period, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void destroy() {
        logger.info("Shutdown scheduled jobs executor");
        scheduler.shutdownNow();
    }
}

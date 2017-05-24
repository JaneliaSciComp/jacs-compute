package org.janelia.jacs2.asyncservice.common;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.slf4j.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class JacsJobRunner {

    private JacsServiceDispatcher jacsServiceDispatcher;
    private ScheduledExecutorService scheduler;
    private Logger logger;
    private int initialDelay;
    private int period;

    JacsJobRunner() {
        // CDI required ctor
    }

    @Inject
    public JacsJobRunner(JacsServiceDispatcher jacsServiceDispatcher,
                         @PropertyValue(name = "service.dispatcher.InitialDelayInSeconds") int initialDelay,
                         @PropertyValue(name = "service.dispatcher.PeriodInSeconds") int period,
                         Logger logger) {
        this.jacsServiceDispatcher = jacsServiceDispatcher;
        this.initialDelay = initialDelay == 0 ? 30 : initialDelay;
        this.period = period == 0 ? 10 : period;
        this.logger = logger;
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("JACS-DISPATCH-%d")
                .setDaemon(true)
                .build();
        this.scheduler = Executors.newScheduledThreadPool(1, threadFactory);
    }

    private void doWork() {
        try {
            jacsServiceDispatcher.dispatchServices();
        } catch (Exception e) {
            logger.error("Critical error - job dispatch failed", e);
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

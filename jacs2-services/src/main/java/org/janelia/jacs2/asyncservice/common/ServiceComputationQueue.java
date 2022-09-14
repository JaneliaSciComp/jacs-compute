package org.janelia.jacs2.asyncservice.common;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.janelia.jacs2.cdi.qualifier.JacsTask;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.slf4j.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class ServiceComputationQueue {

    static boolean runTask(ServiceComputationTask<?> task) {
        task.run();
        return task.isDone();
    }

    private BlockingQueue<ServiceComputationTask<?>> taskQueue;
    private ExecutorService taskExecutor;
    private ScheduledExecutorService taskQueueScheduler;
    private Logger logger;
    private int initialDelay;
    private int period;

    ServiceComputationQueue() {
        // CDI ctor
        taskQueue = new LinkedBlockingQueue<>();
    }

    @Inject
    public ServiceComputationQueue(@JacsTask ExecutorService taskExecutor,
                                   @PropertyValue(name = "service.taskQueue.InitialDelayInMillis") int initialDelay,
                                   @PropertyValue(name = "service.taskQueue.PeriodInMillis") int period,
                                   @PropertyValue(name = "service.taskQueue.ThreadPoolSize") int taskQueuePoolSize,
                                   Logger logger) {
        this();
        this.taskExecutor = taskExecutor;
        this.initialDelay = initialDelay > 0 ? initialDelay : 0;
        this.period = period == 0 ? 10 : period;
        this.logger = logger;
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("JACS-taskQueue-%d")
                .setDaemon(true)
                .build();
        this.taskQueueScheduler = Executors.newScheduledThreadPool(taskQueuePoolSize, threadFactory);
    }

    private void doWork() {
        try {
            cycleThroughAvailableTasks();
        } catch (Exception e) {
            logger.error("Critical error - running computation tasks", e);
        }
    }

    @PostConstruct
    public void initialize() {
        logger.info("Initialize task queue scheduler to run every {}ms with an initial delay of {}ms", period, initialDelay);
        taskQueueScheduler.scheduleAtFixedRate(this::doWork, initialDelay, period, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void destroy() {
        logger.info("Shutdown task queue scheduler");
        taskQueueScheduler.shutdownNow();
    }

    private void cycleThroughAvailableTasks() {
        int n = taskQueue.size();
        for (int i = 0; i < n; i++) {
            try {
                ServiceComputationTask<?> task = taskQueue.take();
                if (task.isReady()) {
                    taskExecutor.execute(() -> {
                        if (!ServiceComputationQueue.runTask(task)) {
                            // if it's not done put it back in the queue
                            try {
                                taskQueue.put(task);
                            } catch (InterruptedException e) {
                                logger.error("Interrupted task {} in the executor thread", task, e);
                            }
                        }
                    });
                } else {
                    // if the task is not ready put it back in the queue
                    taskQueue.put(task);
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    void submit(ServiceComputationTask<?> task) {
        try {
            taskQueue.put(task);
        } catch (InterruptedException e) {
            throw new SuspendedException(e);
        }
    }
}

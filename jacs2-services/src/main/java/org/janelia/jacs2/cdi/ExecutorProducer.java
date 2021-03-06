package org.janelia.jacs2.cdi;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.cdi.AsyncIndex;
import org.slf4j.Logger;

@ApplicationScoped
public class ExecutorProducer {

    private final static int DEFAULT_THREAD_POOL_SIZE = 100;

    @Inject
    private Logger logger;

    @PropertyValue(name = "service.executor.ThreadPoolSize")
    @Inject
    private Integer threadPoolSize;

    @ApplicationScoped
    @Produces
    public ExecutorService createExecutorService() {
        if (threadPoolSize == null || threadPoolSize == 0) {
            threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
        }
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("JACS-%03d")
                .setDaemon(true)
                .build();
        return Executors.newFixedThreadPool(threadPoolSize, threadFactory);
    }

    public void shutdownExecutor(@Disposes @Default ExecutorService executorService) throws InterruptedException {
        logger.info("Shutting down {}", executorService);
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }

    @ApplicationScoped
    @AsyncIndex
    @Produces
    public ExecutorService createIndexingExecutorService() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("JACS-INDEXING-%03d")
                .setDaemon(true)
                .build();
        return Executors.newFixedThreadPool(threadPoolSize, threadFactory);
    }

    public void shutdownIndexingExecutor(@Disposes @AsyncIndex ExecutorService executorService) throws InterruptedException {
        logger.info("Shutting down {}", executorService);
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }

}

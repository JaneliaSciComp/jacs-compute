package org.janelia.jacs2.cdi;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.model.access.cdi.AsyncIndex;
import org.slf4j.Logger;

@ApplicationScoped
public class ExecutorProducer {
    @Inject
    private Logger logger;

    @ApplicationScoped
    @Produces
    @JacsDefault
    public ExecutorService createExecutorService(@IntPropertyValue(name = "service.executor.ThreadPoolSize", defaultValue = 20) Integer threadPoolSize) {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("JACS-%03d")
                .setDaemon(true)
                .build();
        return Executors.newFixedThreadPool(threadPoolSize, threadFactory);
    }

    public void shutdownExecutor(@Disposes @JacsDefault ExecutorService executorService) throws InterruptedException {
        logger.info("Shutting down JACS service executor: {}", executorService);
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }

    @ApplicationScoped
    @AsyncIndex
    @Produces
    public ExecutorService createIndexingExecutorService(@IntPropertyValue(name = "service.indexing.poolSize", defaultValue = 20) Integer indexingPoolSize) {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("JACS-INDEXING-%03d")
                .setDaemon(true)
                .build();
        return Executors.newFixedThreadPool(indexingPoolSize, threadFactory);
    }

    public void shutdownIndexingExecutor(@Disposes @AsyncIndex ExecutorService executorService) throws InterruptedException {
        logger.info("Shutting down async indexing executor: {}", executorService);
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }

}

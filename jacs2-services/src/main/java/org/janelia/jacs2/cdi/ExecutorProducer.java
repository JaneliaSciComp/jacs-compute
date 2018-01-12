package org.janelia.jacs2.cdi;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.janelia.jacs2.cdi.qualifier.GridExecutor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class ExecutorProducer {

    private final static int DEFAULT_THREAD_POOL_SIZE = 100;

    @Inject
    private Logger logger;

    @PropertyValue(name = "service.executor.ThreadPoolSize")
    @Inject
    private Integer threadPoolSize;

    @ApplicationScoped
    @Produces @Default
    public ExecutorService createExecutorService() {
        if (threadPoolSize == null || threadPoolSize == 0) {
            threadPoolSize = DEFAULT_THREAD_POOL_SIZE;
        }
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("JACS-%d")
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
    @Produces @GridExecutor
    public ExecutorService createGridJobExecutorService() {
        return Executors.newCachedThreadPool((runnable) -> {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            // Ensure that we can shut down without these threads getting in the way
            thread.setName("CompletionMessageThread");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void shutdownGridJobExecutor(@Disposes @GridExecutor ExecutorService executorService) throws InterruptedException {
        logger.info("Shutting down {}", executorService);
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }

}

package org.janelia.jacs2.asyncservice.common;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

@ApplicationScoped
public class ServiceComputationQueue {

    static boolean runTask(ServiceComputationTask<?> task) {
        task.run();
        return task.isDone();
    }

    private ExecutorService taskExecutor;
    private ExecutorService queueInspector;
    private BlockingQueue<ServiceComputationTask<?>> taskQueue;

    ServiceComputationQueue() {
        // CDI ctor
        taskQueue = new LinkedBlockingQueue<>();
    }

    @Inject
    public ServiceComputationQueue(ExecutorService taskExecutor,ExecutorService queueInspector) {
        this();
        this.taskExecutor = taskExecutor;
        this.queueInspector = queueInspector;
    }

    @PostConstruct
    void initialize() {
       this.queueInspector.submit(() -> executeTasks());
    }

    private void executeTasks() {
        for (;;) {
            try {
                cycleThroughAvailableTasks();
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                break;
            }
        }
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
                                throw new IllegalStateException(e);
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

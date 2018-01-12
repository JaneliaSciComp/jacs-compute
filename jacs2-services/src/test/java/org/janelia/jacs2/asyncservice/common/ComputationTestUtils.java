package org.janelia.jacs2.asyncservice.common;

import org.slf4j.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ComputationTestUtils {

    public static ServiceComputationFactory createTestServiceComputationFactory(Logger logger) {
        ServiceComputationQueue serviceComputationQueue = mock(ServiceComputationQueue.class);
        doAnswer((invocation -> {
            ServiceComputationTask task = invocation.getArgument(0);
            if (task != null) {
                for (;;) {
                    ServiceComputationQueue.runTask(task);
                    if (task.isDone()) {
                        break;
                    }
                    Thread.sleep(10L);
                }
            }
            return null;
        })).when(serviceComputationQueue).submit(any(ServiceComputationTask.class));
        return new ServiceComputationFactory(serviceComputationQueue, logger);
    }

    public static ThrottledExeJobsQueue createTestThrottledProcessesQueue() {
        ThrottledExeJobsQueue processesQueue = mock(ThrottledExeJobsQueue.class);
        when(processesQueue.add(any(ThrottledJobInfo.class)))
                .then(invocation -> {
                    ThrottledJobInfo jobInfo = invocation.getArgument(0);
                    jobInfo.start();
                    return jobInfo;
                });
        return processesQueue;
    }
}

package org.janelia.jacs2.asyncservice.common;

import org.slf4j.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

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

}

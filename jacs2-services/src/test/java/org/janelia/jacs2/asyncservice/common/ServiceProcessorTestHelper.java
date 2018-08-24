package org.janelia.jacs2.asyncservice.common;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

public class ServiceProcessorTestHelper {

    public static void prepareServiceProcessorMetadataAsRealCall(ServiceProcessor<?>... serviceProcessors) {
        for (ServiceProcessor<?> serviceProcessor : serviceProcessors) {
            when(serviceProcessor.getMetadata()).thenCallRealMethod();
            when(serviceProcessor.createServiceData(any(ServiceExecutionContext.class), anyList())).thenCallRealMethod();
        }
    }

    public static void prepareServiceResultHandlerAsRealCall(ServiceProcessor<?>... serviceProcessors) {
        for (ServiceProcessor<?> serviceProcessor : serviceProcessors) {
            when(serviceProcessor.getResultHandler()).thenCallRealMethod();
        }

    }
}

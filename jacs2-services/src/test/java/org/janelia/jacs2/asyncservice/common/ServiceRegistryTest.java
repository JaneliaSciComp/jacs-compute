package org.janelia.jacs2.asyncservice.common;

import javax.enterprise.inject.Instance;

import com.google.common.collect.ImmutableList;

import org.hamcrest.MatcherAssert;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.jacs2.asyncservice.impl.JacsServiceRegistry;
import org.janelia.model.service.ServiceMetaData;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServiceRegistryTest {
    private ServiceRegistry testServiceRegistry;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);
        @SuppressWarnings("unchecked")
        Instance<ServiceProcessor<?>> anyServiceSource = mock(Instance.class);
        ServiceProcessor<?> registeredService = mock(ServiceProcessor.class);
        testServiceRegistry = new JacsServiceRegistry(anyServiceSource, logger);
        when(registeredService.getMetadata()).thenReturn(createTestServiceMetadata());
        when(anyServiceSource.iterator()).thenReturn(ImmutableList.<ServiceProcessor<?>>of(registeredService).iterator());
    }

    private ServiceMetaData createTestServiceMetadata() {
        ServiceMetaData smd = new ServiceMetaData();
        smd.setServiceName("registered");
        return smd;
    }
    @Test
    public void getMetadataForRegisteredService() {
        ServiceMetaData smd = testServiceRegistry.getServiceMetadata("registered");
        MatcherAssert.assertThat(smd, hasProperty("serviceName", equalTo("registered")));
    }

    @Test
    public void getMetadataForUnregisteredService() {
        ServiceMetaData smd = testServiceRegistry.getServiceMetadata("unregistered");
        assertNull(smd);
    }

}

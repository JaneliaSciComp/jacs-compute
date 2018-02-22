package org.janelia.jacs2.dataservice.cronservice;

import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.dataservice.persistence.JacsScheduledServiceDataPersistence;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsScheduledServiceData;
import org.janelia.model.service.JacsServiceData;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Date;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

public class CronScheduledServiceManagerTest {

    private static final String TEST_QUEUE_ID = "testQueue";

    private JacsScheduledServiceDataPersistence jacsScheduledServiceDataPersistence;
    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private CronScheduledServiceManager cronScheduledServiceManager;

    @Before
    public void setUp() {
        jacsScheduledServiceDataPersistence = Mockito.mock(JacsScheduledServiceDataPersistence.class);
        jacsServiceDataPersistence = Mockito.mock(JacsServiceDataPersistence.class);
        cronScheduledServiceManager = new CronScheduledServiceManager(jacsScheduledServiceDataPersistence, jacsServiceDataPersistence, TEST_QUEUE_ID);
    }

    @Test
    public void scheduleServices() {
        List<JacsScheduledServiceData> testData = ImmutableList.<JacsScheduledServiceData>builder()
                .add(createTestData("j1", "s1", "* */5 */1 * * ?")) // every 5min
                .add(createTestData("j2", "s2", "0 0 */1 * * ?")) // every hour
                .add(createTestData("j3", "s3", "0 59 23 1 * ?"))
                .add(createTestData("j4", "s4", "* 23 * ? * MON-FRI *"))
                .add(createTestData("j5", "s5", null))
                .add(createTestData("j6", null, "* 23 * ? * MON-FRI *"))
                .build();
        Mockito.when(jacsScheduledServiceDataPersistence.findServicesScheduledAtOrBefore(ArgumentMatchers.eq(TEST_QUEUE_ID), ArgumentMatchers.any(Date.class)))
                .thenReturn(testData);
        Mockito.when(jacsScheduledServiceDataPersistence.updateServicesScheduledAtOrBefore(ArgumentMatchers.any(List.class), ArgumentMatchers.any(Date.class)))
                .then(invocation -> invocation.getArgument(0));
        Mockito.when(jacsServiceDataPersistence.createEntity(ArgumentMatchers.any(JacsServiceData.class)))
                .then(invocation -> invocation.getArgument(0));
        List<JacsServiceData> scheduledServices = cronScheduledServiceManager.scheduleServices();
        assertThat(scheduledServices, hasSize(4));
    }

    private JacsScheduledServiceData createTestData(String jobName, String serviceName, String cronDescriptor) {
        JacsScheduledServiceData scheduledService = new JacsScheduledServiceData();
        scheduledService.setName(jobName);
        scheduledService.setServiceName(serviceName);
        scheduledService.setCronScheduleDescriptor(cronDescriptor);
        return scheduledService;
    }
}

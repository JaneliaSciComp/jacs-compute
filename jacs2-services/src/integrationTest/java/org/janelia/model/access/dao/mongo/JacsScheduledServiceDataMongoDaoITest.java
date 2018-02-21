package org.janelia.model.access.dao.mongo;

import org.hamcrest.Matchers;
import org.janelia.model.access.dao.JacsScheduledServiceDataDao;
import org.janelia.model.service.JacsScheduledServiceData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

public class JacsScheduledServiceDataMongoDaoITest extends AbstractMongoDaoITest<JacsScheduledServiceData> {

    private static final String TEST_QUEUE_ID = "testQueue";

    private List<JacsScheduledServiceData> testData = new ArrayList<>();
    private JacsScheduledServiceDataDao testDao;

    @Before
    public void setUp() {
        testDao = new JacsScheduledServiceDataMongoDao(testMongoDatabase, idGenerator);
    }

    @After
    public void tearDown() {
        // delete the data that was created for testing
        deleteAll(testDao, testData);
    }

    @Test
    public void findScheduledServices() {
        int nTestServices = 10;
        List<JacsScheduledServiceData> newServices = createMultipleTestItems(nTestServices);
        testDao.saveAll(newServices);
        Calendar currentCal = Calendar.getInstance();
        Date timeToCheck;
        for (int i = 0; i < nTestServices; i++) {
            timeToCheck = currentCal.getTime();
            for (String testQueueId : new String[] {null, TEST_QUEUE_ID}) {
                verifyScheduledService(timeToCheck, testQueueId, i);
            }
            currentCal.add(Calendar.HOUR_OF_DAY, 1);
        }
        timeToCheck = currentCal.getTime();
        for (String testQueueId : new String[] {null, TEST_QUEUE_ID}) {
            verifyScheduledService(timeToCheck, testQueueId, nTestServices);
        }
    }

    @Test
    public void findScheduledServicesAssociatedWithAQueue() {
        int nTestServices = 10;
        List<JacsScheduledServiceData> newServices = createMultipleTestItems(nTestServices);
        newServices.forEach(sd -> {
            sd.setServiceQueueId(TEST_QUEUE_ID);
            testDao.save(sd);
        });
        Calendar currentCal = Calendar.getInstance();
        Date timeToCheck;
        for (int i = 0; i < nTestServices; i++) {
            timeToCheck = currentCal.getTime();
            verifyScheduledService(timeToCheck, null, 0);
            verifyScheduledService(timeToCheck, TEST_QUEUE_ID, i);
            verifyScheduledService(timeToCheck, "someotherqueue", 0);
            currentCal.add(Calendar.HOUR_OF_DAY, 1);
        }
        timeToCheck = currentCal.getTime();
        verifyScheduledService(timeToCheck, null, 0);
        verifyScheduledService(timeToCheck, TEST_QUEUE_ID, nTestServices);
        verifyScheduledService(timeToCheck, "someotherqueue", 0);
    }

    private void verifyScheduledService(Date timestamp, String testQueueId, int expectedResults) {
        List<JacsScheduledServiceData> scheduledServices = testDao.findServiceScheduledAtOrBefore(testQueueId, timestamp, false);
        assertThat(scheduledServices, hasSize(expectedResults));
        assertThat(scheduledServices, everyItem(Matchers.hasProperty("nextStartTime", Matchers.lessThanOrEqualTo(timestamp))));
        assertThat(scheduledServices, everyItem(Matchers.hasProperty("disabled", Matchers.equalTo(false))));
    }

    @Override
    protected List<JacsScheduledServiceData> createMultipleTestItems(int nItems) {
        List<JacsScheduledServiceData> testItems = new ArrayList<>();
        Calendar currentCal = Calendar.getInstance();
        Date now = currentCal.getTime();
        for (int i = 0; i < nItems; i++) {
            currentCal.add(Calendar.HOUR_OF_DAY, 1);
            testItems.add(createTestService("job-" + i, now, currentCal.getTime()));
        }
        return testItems;
    }

    private JacsScheduledServiceData createTestService(String scheduledServiceName, Date lastRunDate, Date nextScheduledDate) {
        JacsScheduledServiceData scheduledService = new JacsScheduledServiceData();
        scheduledService.setName(scheduledServiceName);
        scheduledService.setServiceName(scheduledServiceName + "Processor");
        scheduledService.setLastStartTime(lastRunDate);
        scheduledService.setNextStartTime(nextScheduledDate);
        testData.add(scheduledService);
        return scheduledService;
    }

}

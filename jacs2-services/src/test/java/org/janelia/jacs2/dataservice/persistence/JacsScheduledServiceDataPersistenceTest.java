package org.janelia.jacs2.dataservice.persistence;

import com.google.common.collect.ImmutableList;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.janelia.model.access.dao.JacsScheduledServiceDataDao;
import org.janelia.model.service.JacsScheduledServiceData;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.enterprise.inject.Instance;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertThat;

public class JacsScheduledServiceDataPersistenceTest {

    private Instance<JacsScheduledServiceDataDao> daoSource;
    private JacsScheduledServiceDataDao dao;
    private JacsScheduledServiceDataPersistence jacsScheduledServiceDataPersistence;

    @Before
    public void setUp() {
        daoSource = Mockito.mock(Instance.class);
        dao = Mockito.mock(JacsScheduledServiceDataDao.class);
        Mockito.when(daoSource.get()).thenReturn(dao);
        jacsScheduledServiceDataPersistence = new JacsScheduledServiceDataPersistence(daoSource);
    }

    @Test
    public void updateScheduledServices() {
        int nTestServices = 10;
        List<JacsScheduledServiceData> testServices = IntStream.range(0, nTestServices)
                .mapToObj(i -> createTestData(i))
                .collect(Collectors.toList());
        Mockito.when(dao.updateServiceScheduledTime(ArgumentMatchers.any(JacsScheduledServiceData.class), ArgumentMatchers.any(Date.class))).then(invocation -> {
            JacsScheduledServiceData sd = invocation.getArgument(0);
            if (sd.getId().intValue() % 2 == 0) {
                sd.setLastStartTime(invocation.getArgument(1));
                return Optional.of(sd);
            } else {
                return Optional.empty();
            }
        });
        Date testScheduledDate = new Date();
        List<JacsScheduledServiceData> scheduledServices = jacsScheduledServiceDataPersistence.updateServicesScheduledAtOrBefore(testServices, testScheduledDate);
        assertThat(scheduledServices, Matchers.everyItem(Matchers.hasProperty("id", new TypeSafeMatcher<Integer>() {
            @Override
            protected boolean matchesSafely(Integer item) {
                return item.intValue() % 2 == 0;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("that is even");
            }
        })));
        assertThat(scheduledServices, Matchers.everyItem(Matchers.hasProperty("lastStartTime", Matchers.sameInstance(testScheduledDate))));
    }

    private JacsScheduledServiceData createTestData(Number id) {
        JacsScheduledServiceData testData = new JacsScheduledServiceData();
        testData.setId(id);
        return testData;
    }
}

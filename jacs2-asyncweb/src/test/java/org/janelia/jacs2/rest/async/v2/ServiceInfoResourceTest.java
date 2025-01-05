package org.janelia.jacs2.rest.async.v2;

import jakarta.ws.rs.core.Response;

import com.google.common.collect.ImmutableList;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.page.SortCriteria;
import org.janelia.model.jacs2.page.SortDirection;
import org.janelia.model.security.User;
import org.janelia.model.service.JacsServiceData;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;

public class ServiceInfoResourceTest extends AbstractAsyncServicesAppResourceTest {

    private static final String TEST_USERNAME = "test";

    @SuppressWarnings("unchecked")
    @Test
    public void searchServices() {
        User testUser = new User();
        testUser.setKey("user:" + TEST_USERNAME);
        testUser.setName(TEST_USERNAME);
        Mockito.when(dependenciesProducer.getSubjectDao().findSubjectByNameOrKey(TEST_USERNAME))
                .thenReturn(testUser);
        Response testResponse = target()
                .path("services")
                .queryParam("sort-by", " name, ownerKey asc, state desc, priority asc")
                .request()
                .header("username", TEST_USERNAME)
                .get();
        assertEquals(200, testResponse.getStatus());
        Mockito.verify(dependenciesProducer.getJacsServiceDataManager()).searchServices(
                any(JacsServiceData.class),
                any(DataInterval.class),
                argThat(argument -> ImmutableList.of(
                            new SortCriteria("name", SortDirection.ASC),
                            new SortCriteria("ownerKey", SortDirection.ASC),
                            new SortCriteria("state", SortDirection.DESC),
                            new SortCriteria("priority", SortDirection.ASC)
                    ).equals(argument.getSortCriteria()))
        );

    }
}

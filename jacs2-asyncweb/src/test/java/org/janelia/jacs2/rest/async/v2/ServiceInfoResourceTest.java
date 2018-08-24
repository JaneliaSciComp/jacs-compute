package org.janelia.jacs2.rest.async.v2;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ResourceConfig;
import org.hamcrest.beans.HasPropertyWithValue;
import org.janelia.jacs2.app.AsyncServicesApp;
import org.janelia.jacs2.app.JAXAsyncAppConfig;
import org.janelia.jacs2.testrest.AbstractCdiInjectedResourceTest;
import org.janelia.jacs2.testrest.TestResourceBinder;
import org.janelia.jacs2.testrest.TestResourceDependenciesProducer;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.SortCriteria;
import org.janelia.model.jacs2.page.SortDirection;
import org.janelia.model.security.User;
import org.janelia.model.service.JacsServiceData;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;

public class ServiceInfoResourceTest extends AbstractAsyncServicesAppResourceTest {

    private static final String TEST_USERNAME = "test";

    @SuppressWarnings("unchecked")
    @Test
    public void searchServices() {
        User testUser = new User();
        testUser.setKey("user:" + TEST_USERNAME);
        testUser.setName(TEST_USERNAME);
        Mockito.when(dependenciesProducer.getLegacyDomainDao().getSubjectByNameOrKey(TEST_USERNAME))
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

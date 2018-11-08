package org.janelia.jacs2.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.jacs2.auth.JacsSecurityContext;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.security.Subject;
import org.janelia.model.security.User;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import java.lang.reflect.Method;
import java.net.URI;

import static org.mockito.ArgumentMatchers.any;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        AuthFilter.class,
})
public class AuthFilterTest {

    @Mock
    private LegacyDomainDao dao;
    @Mock
    private JwtDecoder jwtDecoder;
    @Mock
    private Logger logger;
    @Mock
    private ResourceInfo resourceInfo;
    private AuthFilter authFilter;

    @Before
    public void setUp() {
        authFilter = new AuthFilter();
        Whitebox.setInternalState(authFilter, "dao", dao);
        Whitebox.setInternalState(authFilter, "logger", logger);
        Whitebox.setInternalState(authFilter, "resourceInfo", resourceInfo);
        Whitebox.setInternalState(authFilter, "jwtDecoder", jwtDecoder);
    }

    @Test
    public void filterWithAuthorizationHeader() {
        final String testToken = "testToken";
        final String testUserName = "testUser";

        @RequireAuthentication
        class TestResource {
            void m() {};
        }
        TestResource testResource = new TestResource();

        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        Mockito.when(resourceInfo.getResourceMethod()).then(invocation -> Whitebox.getMethod(TestResource.class, "m"));
        Mockito.when(requestContext.getHeaderString("Authorization")).thenReturn("Bearer " + testToken);
        Mockito.when(jwtDecoder.decode(testToken)).thenReturn(createTestJWT(testUserName, System.currentTimeMillis()/1000 + 200));
        Mockito.when(dao.getSubjectByNameOrKey(testUserName)).then(invocation -> {
            String usernameArg = invocation.getArgument(0);
            Subject subject = new User();
            subject.setKey("user:" + usernameArg);
            return subject;
        });
        UriInfo uriInfo = Mockito.mock(UriInfo.class);
        Mockito.when(uriInfo.getRequestUri()).thenReturn(URI.create("http://test:1000"));
        Mockito.when(requestContext.getUriInfo()).thenReturn(uriInfo);
        authFilter.filter(requestContext);
        Mockito.verify(requestContext).setSecurityContext(any(JacsSecurityContext.class));
    }

    private JWT createTestJWT(String user, long expInSec) {
        JWT jwt = new JWT();
        jwt.userName = user;
        jwt.expInSeconds = expInSec;
        return jwt;
    }
}

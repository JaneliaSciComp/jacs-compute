package org.janelia.jacs2.filter;

import org.janelia.jacs2.auth.JWTProvider;
import org.janelia.jacs2.auth.JacsSecurityContext;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.security.GroupRole;
import org.janelia.model.security.Subject;
import org.janelia.model.security.User;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        AuthFilter.class,
})
@PowerMockIgnore({"javax.crypto.*" }) // Due to https://github.com/powermock/powermock/issues/294
public class AuthFilterTest {

    private static final String TEST_KEY = "TESTKEY";
    private static final String TEST_SYSTEM_USER = "TESTUSER";

    @Mock
    private SubjectDao subjectDao;
    @Mock
    private JWTProvider jwtProvider;
    @Mock
    private Logger logger;
    @Mock
    private ResourceInfo resourceInfo;
    private AuthFilter authFilter;

    @Before
    public void setUp() {
        authFilter = new AuthFilter();
        Whitebox.setInternalState(authFilter, "subjectDao", subjectDao);
        Whitebox.setInternalState(authFilter, "logger", logger);
        Whitebox.setInternalState(authFilter, "resourceInfo", resourceInfo);
        Whitebox.setInternalState(authFilter, "jwtProvider", jwtProvider);
        Whitebox.setInternalState(authFilter, "apiKey", TEST_KEY);
        Whitebox.setInternalState(authFilter, "systemUser", TEST_SYSTEM_USER);
    }

    @Test
    public void noAuthRequired() {
        class TestResource {
            void m() {
            }
        }
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        Mockito.when(resourceInfo.getResourceMethod()).then(invocation -> Whitebox.getMethod(TestResource.class, "m"));
        authFilter.filter(requestContext);
        Mockito.verifyNoMoreInteractions(requestContext, subjectDao, jwtProvider);
    }

    @Test
    public void filterWithHeaderUsername() {
        @RequireAuthentication
        class TestResource {
            void m() {
            }
        }
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        Mockito.when(resourceInfo.getResourceMethod()).then(invocation -> Whitebox.getMethod(TestResource.class, "m"));
        final String testUserName = "thisuser";
        Mockito.when(requestContext.getHeaders())
                .thenReturn(new MultivaluedHashMap<>(ImmutableMap.of("UserName", testUserName)));
        Mockito.when(subjectDao.findSubjectByNameOrKey(testUserName)).then(invocation -> {
            String usernameArg = invocation.getArgument(0);
            Subject subject = new User();
            subject.setKey("user:" + usernameArg);
            return subject;
        });
        UriInfo uriInfo = Mockito.mock(UriInfo.class);
        Mockito.when(uriInfo.getRequestUri()).thenReturn(URI.create("http://test:1000"));
        Mockito.when(requestContext.getUriInfo()).thenReturn(uriInfo);

        authFilter.filter(requestContext);
        Mockito.verify(requestContext, Mockito.times(2)).getHeaders();
        Mockito.verify(requestContext).setSecurityContext(any(JacsSecurityContext.class));
        Mockito.verify(requestContext, Mockito.times(1)).getUriInfo();
        Mockito.verify(requestContext).setSecurityContext(any(JacsSecurityContext.class));
        Mockito.verify(subjectDao).findSubjectByNameOrKey(testUserName);

        Mockito.verifyNoMoreInteractions(requestContext, subjectDao, jwtProvider);
    }

    @Test
    public void filterWithHeaderUsernameAnRunAs() {
        @RequireAuthentication
        class TestResource {
            void m() {
            }
        }
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        Mockito.when(resourceInfo.getResourceMethod()).then(invocation -> Whitebox.getMethod(TestResource.class, "m"));
        final String testUserName = "thisuser";
        final String runAsThisUser = "runasthis";
        Mockito.when(requestContext.getHeaders())
                .thenReturn(new MultivaluedHashMap<>(ImmutableMap.of(
                        "UserName", testUserName,
                        "RunAsUser", runAsThisUser
                )));
        AtomicLong idGen = new AtomicLong(1);
        Mockito.when(subjectDao.findSubjectByNameOrKey(anyString())).then(invocation -> {
            String usernameArg = invocation.getArgument(0);
            Subject subject = new User() {{
                setUserGroupRole("group:admin", GroupRole.Reader);
            }};
            subject.setId(idGen.getAndIncrement());
            subject.setKey("user:" + usernameArg);
            return subject;
        });
        UriInfo uriInfo = Mockito.mock(UriInfo.class);
        Mockito.when(uriInfo.getRequestUri()).thenReturn(URI.create("http://test:1000"));
        Mockito.when(requestContext.getUriInfo()).thenReturn(uriInfo);

        authFilter.filter(requestContext);
        Mockito.verify(requestContext, Mockito.times(2)).getHeaders();
        Mockito.verify(requestContext).setSecurityContext(any(JacsSecurityContext.class));
        Mockito.verify(requestContext, Mockito.times(1)).getUriInfo();
        Mockito.verify(requestContext).setSecurityContext(any(JacsSecurityContext.class));
        Mockito.verify(subjectDao).findSubjectByNameOrKey(testUserName);
        Mockito.verify(subjectDao).findSubjectByNameOrKey(runAsThisUser);

        Mockito.verifyNoMoreInteractions(requestContext, subjectDao, jwtProvider);
    }

    @Test
    public void filterWithHeaderUsernameWithoutAdminAnRunAs() {
        @RequireAuthentication
        class TestResource {
            void m() {
            }
        }
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        Mockito.when(resourceInfo.getResourceMethod()).then(invocation -> Whitebox.getMethod(TestResource.class, "m"));
        final String testUserName = "thisuser";
        final String runAsThisUser = "runasthis";
        Mockito.when(requestContext.getHeaders())
                .thenReturn(new MultivaluedHashMap<>(ImmutableMap.of(
                        "UserName", testUserName,
                        "RunAsUser", runAsThisUser
                )));
        AtomicLong idGen = new AtomicLong(1);
        Mockito.when(subjectDao.findSubjectByNameOrKey(anyString())).then(invocation -> {
            String usernameArg = invocation.getArgument(0);
            Subject subject = new User();
            subject.setId(idGen.getAndIncrement());
            subject.setKey("user:" + usernameArg);
            return subject;
        });
        UriInfo uriInfo = Mockito.mock(UriInfo.class);
        Mockito.when(uriInfo.getRequestUri()).thenReturn(URI.create("http://test:1000"));
        Mockito.when(requestContext.getUriInfo()).thenReturn(uriInfo);

        authFilter.filter(requestContext);
        Mockito.verify(requestContext, Mockito.times(2)).getHeaders();
        Mockito.verify(requestContext).abortWith(argThat(argument -> argument.getStatus() == 403));
        Mockito.verify(subjectDao).findSubjectByNameOrKey(testUserName);
        Mockito.verify(subjectDao).findSubjectByNameOrKey(runAsThisUser);

        Mockito.verifyNoMoreInteractions(requestContext, subjectDao, jwtProvider);
    }

    @Test
    public void filterWithJWTAuthorizationHeader() {
        final String testToken = "testToken";
        final String testUserName = "testUser";

        @RequireAuthentication
        class TestResource {
            void m() {
            }
        }
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        Mockito.when(resourceInfo.getResourceMethod()).then(invocation -> Whitebox.getMethod(TestResource.class, "m"));
        Mockito.when(requestContext.getHeaders())
                .thenReturn(new MultivaluedHashMap<>(ImmutableMap.of("Authorization", "Bearer " + testToken)));
        Mockito.when(jwtProvider.decodeJWT(testToken)).thenReturn(createTestJWT(testUserName));
        Mockito.when(subjectDao.findSubjectByNameOrKey(testUserName)).then(invocation -> {
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

    private Map<String,String> createTestJWT(String user) {
        Map<String,String> claims = new HashMap<>();
        claims.put(JWTProvider.USERNAME_CLAIM, user);
        return claims;
    }

    @Test
    public void filterWithAPIKEYAuthorizationHeader() {
        class TestResource {
            @RequireAuthentication
            void m() {
            }
        }
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        Mockito.when(resourceInfo.getResourceMethod()).then(invocation -> Whitebox.getMethod(TestResource.class, "m"));
        Mockito.when(requestContext.getHeaders())
                .thenReturn(new MultivaluedHashMap<>(ImmutableMap.of("Authorization", "APIKEY " + TEST_KEY)));
        UriInfo uriInfo = Mockito.mock(UriInfo.class);
        Mockito.when(uriInfo.getRequestUri()).thenReturn(URI.create("http://test:1000"));
        Mockito.when(requestContext.getUriInfo()).thenReturn(uriInfo);
        authFilter.filter(requestContext);
        Mockito.verify(requestContext).setSecurityContext(any(JacsSecurityContext.class));
    }

    @Test
    public void invalidAPIKEYInAuthorizationHeader() {
        @RequireAuthentication
        class TestResource {
            void m() {
            }
        }
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        Mockito.when(resourceInfo.getResourceMethod()).then(invocation -> Whitebox.getMethod(TestResource.class, "m"));
        Mockito.when(requestContext.getHeaders())
                .thenReturn(new MultivaluedHashMap<>(ImmutableMap.of("Authorization", "APIKEY badkey")));
        UriInfo uriInfo = Mockito.mock(UriInfo.class);
        Mockito.when(uriInfo.getRequestUri()).thenReturn(URI.create("http://test:1000"));
        Mockito.when(requestContext.getUriInfo()).thenReturn(uriInfo);
        authFilter.filter(requestContext);
        Mockito.verify(requestContext).abortWith(argThat(argument -> argument.getStatus() == 401));
    }

    @Test
    public void noAuthorizationHeader() {
        @RequireAuthentication
        class TestResource {
            void m() {
            }
        }
        ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
        Mockito.when(resourceInfo.getResourceMethod()).then(invocation -> Whitebox.getMethod(TestResource.class, "m"));
        Mockito.when(requestContext.getHeaders())
                .thenReturn(new MultivaluedHashMap<>(ImmutableMap.of()));
        UriInfo uriInfo = Mockito.mock(UriInfo.class);
        Mockito.when(uriInfo.getRequestUri()).thenReturn(URI.create("http://test:1000"));
        Mockito.when(requestContext.getUriInfo()).thenReturn(uriInfo);
        authFilter.filter(requestContext);
        Mockito.verify(requestContext).abortWith(argThat(argument -> argument.getStatus() == 401));
    }

}

package org.janelia.jacs2.filter;

import java.lang.reflect.Method;
import java.util.Optional;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.auth.JWTProvider;
import org.janelia.jacs2.auth.JacsSecurityContext;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.security.GroupRole;
import org.janelia.model.security.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authorization filter for async and sync web services.
 *
 * JACSv2 relies on an API Gateway for performing JWT-based authentication. It expects a header on every request
 * with key HEADER_USERNAME. The value is assumed to be an authenticated username (or subject key).
 *
 * Optionally, the caller could also provide a second header with key HEADER_RUNASUSER. This may be a user or group name,
 * or key. If this is provided, the actual user (i.e. specified by HEADER_USERNAME) must be an administrator capable of
 * running as other users. If not, a 403 FORBIDDEN is returned.
 *
 * If a runasuser is successfully authorized then it will be used going forward for all operations (retrieving objections,
 * creating objects, etc.)
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Priority(Priorities.AUTHENTICATION)
@ApplicationScoped
public class AuthFilter implements ContainerRequestFilter {
    private static final Logger LOG = LoggerFactory.getLogger(AuthFilter.class);
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String HEADER_USERNAME = "username";
    private static final String HEADER_RUNASUSER = "runasuser";
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String APIKEY_PREFIX = "APIKEY ";

    @Inject
    private SubjectDao subjectDao;
    @Inject
    private JWTProvider jwtProvider;
    @PropertyValue(name = "JACS.SystemAppUserName")
    @Inject
    private String systemUser;
    @PropertyValue(name = "JACS.ApiKey")
    @Inject
    private String apiKey;
    @Context
    private ResourceInfo resourceInfo;

    @Override
    public void filter(ContainerRequestContext requestContext) {
        Method method = resourceInfo.getResourceMethod();
        boolean authenticationRequired = method.isAnnotationPresent(RequireAuthentication.class) ||
                method.getDeclaringClass().isAnnotationPresent(RequireAuthentication.class);
        if (!authenticationRequired) {
            // everybody is allowed to access the method
            return;
        }
        String authUserName = getAuthUserName(requestContext).orElse("");
        Subject authenticatedSubject;
        Response subjectCheckResponse;
        if (StringUtils.isNotBlank(authUserName)) {
            authenticatedSubject = subjectDao.findSubjectByNameOrKey(authUserName);
            if (authenticatedSubject == null) {
                LOG.warn("Invalid username parameter passed in for authentication - no entry found for {}", authUserName);
                subjectCheckResponse = Response.status(Response.Status.UNAUTHORIZED)
                        .entity(new ErrorResponse("Invalid authentication"))
                        .build();
            } else {
                subjectCheckResponse = null;
            }
        } else {
            // try to extract API KEY
            boolean validApiKeyFound = getApiKeyFromAuthorizationHeader(requestContext)
                    .map(headerApiKey -> {
                        if (StringUtils.isNotBlank(headerApiKey)) {
                            if (headerApiKey.equals(apiKey)) {
                                return true;
                            } else {
                                LOG.warn("Header APIKEY {} does not match the configured key", headerApiKey);
                                return false;
                            }
                        } else {
                            LOG.warn("Invalid APIKEY in the authorization header");
                            return false;
                        }
                    })
                    .orElse(false);
            if (!validApiKeyFound) {
                authenticatedSubject = null;
                subjectCheckResponse = Response.status(Response.Status.UNAUTHORIZED)
                        .entity(new ErrorResponse("Invalid authentication"))
                        .build();
            } else {
                subjectCheckResponse = null;
                // create a dummy principal with no specific key and/or ID
                // As a note the authenticated user created for an API key authenticated request
                // does not have admin privileges for now.
                authenticatedSubject = new Subject() {{
                    setName(systemUser);
                    addRoles(GroupRole.Reader, GroupRole.Writer, GroupRole.Admin);
                }};
            }
        }
        if (subjectCheckResponse != null) {
            requestContext.abortWith(subjectCheckResponse);
            return;
        }

        String runAsUserName = getSingleHeaderValue(requestContext, HEADER_RUNASUSER).orElse(null);
        Subject authorizedSubject;

        if (StringUtils.isNotBlank(runAsUserName)) {
            authorizedSubject = subjectDao.findSubjectByNameOrKey(runAsUserName);
            if (authorizedSubject == null) {
                // if "run as" is specified it must be a valid user
                LOG.warn("Invalid run-as user specified in header {}: {}", HEADER_RUNASUSER, runAsUserName);
                requestContext.abortWith(
                        Response.status(Response.Status.FORBIDDEN)
                                .entity(new ErrorResponse("Unauthorized access"))
                                .build()
                );
                return;
            }
            if (!authorizedSubject.getId().equals(authenticatedSubject.getId())) {
                // if the user it's trying to run the job as somebody else it must have admin privileges
                // otherwise if they are the same we should not care
                if (!authenticatedSubject.hasReadPrivilege()) {
                    LOG.warn("User {} is not authorized to act as subject {}", authUserName, runAsUserName);
                    requestContext.abortWith(
                            Response.status(Response.Status.FORBIDDEN)
                                    .entity(new ErrorResponse("Unauthorized access"))
                                    .build()
                    );
                    return;
                }
            }
        } else {
            authorizedSubject = authenticatedSubject;
        }
        JacsSecurityContext securityContext = new JacsSecurityContext(authenticatedSubject,
                authorizedSubject,
                "https".equals(requestContext.getUriInfo().getRequestUri().getScheme()),
                "");
        requestContext.setSecurityContext(securityContext);
    }

    private Optional<String> getAuthUserName(ContainerRequestContext requestContext) {
        return getSingleHeaderValue(requestContext, HEADER_USERNAME)
                .map(username -> Optional.of(username))
                .orElseGet(() -> getUserNameFromAuthorizationHeader(requestContext));
    }

    private Optional<String> getSingleHeaderValue(ContainerRequestContext requestContext, String headerName) {
        return requestContext.getHeaders().entrySet().stream()
                .filter(e -> StringUtils.equalsIgnoreCase(e.getKey(), headerName) && e.getValue() != null)
                .flatMap(e -> e.getValue().stream())
                .findFirst();
    }

    private Optional<String> getUserNameFromAuthorizationHeader(ContainerRequestContext requestContext) {
        return getSingleHeaderValue(requestContext, AUTHORIZATION_HEADER)
                .flatMap(authHeader -> {
                    if (StringUtils.startsWithIgnoreCase(authHeader, BEARER_PREFIX)) {
                        return Optional.of(authHeader.substring(BEARER_PREFIX.length()).trim());
                    } else {
                        return Optional.empty();
                    }
                })
                .filter(StringUtils::isNotBlank)
                .map(token -> jwtProvider.decodeJWT(token))
                .map(jwt -> jwt.get(JWTProvider.USERNAME_CLAIM));
    }

    private Optional<String> getApiKeyFromAuthorizationHeader(ContainerRequestContext requestContext) {
        return getSingleHeaderValue(requestContext, AUTHORIZATION_HEADER)
                .flatMap(authHeader -> {
                    if (StringUtils.startsWithIgnoreCase(authHeader, APIKEY_PREFIX)) {
                        return Optional.of(authHeader.substring(APIKEY_PREFIX.length()).trim());
                    } else {
                        return Optional.empty();
                    }
                });
    }
}

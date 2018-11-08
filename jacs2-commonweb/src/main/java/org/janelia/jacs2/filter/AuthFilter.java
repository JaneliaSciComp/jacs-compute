package org.janelia.jacs2.filter;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.auth.JacsSecurityContext;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.security.Subject;
import org.janelia.model.security.util.SubjectUtils;
import org.slf4j.Logger;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.lang.reflect.Method;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
public class AuthFilter implements ContainerRequestFilter {
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String HEADER_USERNAME = "username";
    private static final String HEADER_RUNASUSER = "runasuser";

    @Inject
    private LegacyDomainDao dao;
    @Inject
    private JwtDecoder jwtDecoder;
    @Inject
    private Logger logger;
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
        String authUserName = getSingleHeaderValue(requestContext, HEADER_USERNAME)
                .orElseGet(() -> getUserNameFromAuthorizationHeader(requestContext).orElse(""));
        if (StringUtils.isBlank(authUserName)) {
            logger.warn("Null or empty username parameter passed in header {} for authentication", HEADER_USERNAME);
            requestContext.abortWith(
                    Response.status(Response.Status.UNAUTHORIZED)
                            .entity(new ErrorResponse("Invalid user name"))
                            .build()
            );
            return;
        }
        Subject authenticatedUser = dao.getSubjectByNameOrKey(authUserName);
        if (authenticatedUser == null) {
            logger.warn("Invalid username parameter passed in for authentication - no entry found for {}", authUserName);
            requestContext.abortWith(
                    Response.status(Response.Status.UNAUTHORIZED)
                            .entity(new ErrorResponse("Invalid user name"))
                            .build()
            );
            return;
        }

        String runAsUserName = requestContext.getHeaderString(HEADER_RUNASUSER);
        Subject authorizedSubject;

        if (StringUtils.isNotBlank(runAsUserName)) {
            authorizedSubject = dao.getSubjectByNameOrKey(runAsUserName);
            if (authorizedSubject == null) {
                // if "run as" is specified it must be a valid user
                logger.warn("Invalid run-as user specified in header {}: {}", HEADER_RUNASUSER, runAsUserName);
                requestContext.abortWith(
                        Response.status(Response.Status.FORBIDDEN)
                                .entity(new ErrorResponse("Unauthorized access"))
                                .build()
                );
                return;
            }
            if (!authorizedSubject.getId().equals(authenticatedUser.getId())) {
                // if the user it's trying to run the job as somebody else it must have admin privileges
                // otherwise if they are the same we should not care
                if (!SubjectUtils.isAdmin(authenticatedUser)) {
                    logger.warn("User {} is not authorized to act as subject {}", authUserName, runAsUserName);
                    requestContext.abortWith(
                            Response.status(Response.Status.FORBIDDEN)
                                    .entity(new ErrorResponse("Unauthorized access"))
                                    .build()
                    );
                    return;
                }
            }
        } else {
            authorizedSubject = authenticatedUser;
        }
        JacsSecurityContext securityContext = new JacsSecurityContext(authenticatedUser,
                authorizedSubject,
                "https".equals(requestContext.getUriInfo().getRequestUri().getScheme()),
                "");
        requestContext.setSecurityContext(securityContext);
    }

    private Optional<String> getSingleHeaderValue(ContainerRequestContext requestContext, String headerName) {
        String headerValue = requestContext.getHeaderString(headerName);
        return StringUtils.isNotBlank(headerValue) ? Optional.of(headerValue) : Optional.empty();
    }

    private Optional<String> getUserNameFromAuthorizationHeader(ContainerRequestContext requestContext) {
        return getSingleHeaderValue(requestContext, AUTHORIZATION_HEADER)
                .flatMap(authHeader -> {
                    if (StringUtils.startsWithIgnoreCase(authHeader, "Bearer ")) {
                        return Optional.of(authHeader.substring("Bearer ".length()).trim());
                    } else {
                        return Optional.empty();
                    }
                })
                .filter(StringUtils::isNotBlank)
                .map(token -> jwtDecoder.decode(token))
                .filter(jwt -> jwt.isValid())
                .map(jwt -> jwt.userName);
    }
}

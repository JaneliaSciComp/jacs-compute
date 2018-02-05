package org.janelia.jacs2.filter;

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
    private static final String HEADER_USERNAME = "username";
    private static final String HEADER_RUNASUSER = "runasuser";

    @Inject
    private LegacyDomainDao dao;
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
        String authUserName = requestContext.getHeaderString(HEADER_USERNAME);
        if (StringUtils.isBlank(authUserName)) {
            logger.warn("Null or empty username parameter passed in header {} for authentication", HEADER_USERNAME);
            requestContext.abortWith(
                    Response.status(Response.Status.UNAUTHORIZED)
                            .entity(new ErrorResponse("Invalid user name"))
                            .build()
            );
        }
        Subject authenticatedUser = dao.getSubjectByNameOrKey(authUserName);
        if (authenticatedUser == null) {
            logger.warn("Invalid username parameter passed in for authentication - no entry found for {}", authUserName);
            requestContext.abortWith(
                    Response.status(Response.Status.UNAUTHORIZED)
                            .entity(new ErrorResponse("Invalid user name"))
                            .build()
            );
            throw new SecurityException("Invalid user name");
        }

        String runAsUserName = requestContext.getHeaderString(HEADER_RUNASUSER);
        Subject authorizedSubject;

        if (StringUtils.isNotBlank(runAsUserName)) {
            // Attempt to "run as" another user or group
            if (!SubjectUtils.isAdmin(authenticatedUser)) {
                logger.debug("User {} is not authorized to act as subject {}", authUserName, runAsUserName);
                requestContext.abortWith(
                        Response.status(Response.Status.UNAUTHORIZED)
                                .entity(new ErrorResponse("Unauthorized access"))
                                .build()
                );
            }
            authorizedSubject = dao.getSubjectByNameOrKey(runAsUserName);
            if (authorizedSubject == null) {
                logger.debug("Invalid run-as user specified in header {}: {}", HEADER_RUNASUSER, runAsUserName);
                requestContext.abortWith(
                        Response.status(Response.Status.UNAUTHORIZED)
                                .entity(new ErrorResponse("Unauthorized access"))
                                .build()
                );
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
}

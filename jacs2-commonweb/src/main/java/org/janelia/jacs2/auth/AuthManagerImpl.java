package org.janelia.jacs2.auth;

import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.security.Subject;
import org.janelia.model.security.User;
import org.janelia.model.security.util.SubjectUtils;
import org.slf4j.Logger;

import javax.enterprise.context.RequestScoped;
import javax.enterprise.inject.Default;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

/**
 * Manages user authorization for async and sync web services.
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
@RequestScoped
@Default
public class AuthManagerImpl implements AuthManager {

    private static final String HEADER_USERNAME = "username";
    private static final String HEADER_RUNASUSER = "runasuser";

    private static final String ATTRIBUTE_SUBJECT_KEY = "subjectKey";
    private static final String ATTRIBUTE_AUTH_USER = "authUser";
    private static final String ATTRIBUTE_SUBJECT = "subject";

    @Inject
    private Logger log;

    @Inject
    private LegacyDomainDao dao;

    @Context
    private HttpServletRequest request;

    public AuthManagerImpl() {

    }

    /**
     * Perform authorization on the request's username and runaskey headers.
     * If HEADER_USERNAME is not provided or not a valid user, the response will be UNAUTHORIZED.
     * If there is any other authorization issue, the response will be FORBIDDEN.
     * @return the runaskey or, if null, the authenticated username
     */
    public String authorize(String... requiredGroups) {

        String username = request.getHeader(HEADER_USERNAME);
        if (username == null) {
            log.debug("No authorize user specified in header {}", HEADER_USERNAME);
            throw new WebApplicationException(Response.Status.UNAUTHORIZED);
        }

        User authUser = dao.getUserByNameOrKey(username);
        if (authUser == null) {
            log.debug("Invalid authorize user specified in header {}", HEADER_USERNAME);
            throw new WebApplicationException(Response.Status.UNAUTHORIZED);
        }

        request.setAttribute(ATTRIBUTE_AUTH_USER, authUser);

        String subjectKey = request.getHeader(HEADER_RUNASUSER);
        Subject subject = null;

        if (subjectKey!=null) {
            // Attempt to "run as" another user or group

            if (!SubjectUtils.isAdmin(authUser)) {
                log.debug("User {} is not authorized to act as subject {}", username, subjectKey);
                throw new WebApplicationException(Response.Status.FORBIDDEN);
            }

            subject = dao.getSubjectByNameOrKey(subjectKey);
            if (subject == null) {
                log.debug("Invalid run-as user specified in header {}: {}", HEADER_RUNASUSER, subjectKey);
                throw new WebApplicationException(Response.Status.FORBIDDEN);
            }

        }
        else {
            subjectKey = username;
            subject = authUser;
        }

        if (requiredGroups != null && requiredGroups.length>0) {
            for(String requiredGroup : requiredGroups) {
                if (!SubjectUtils.subjectIsInGroup(subject, requiredGroup)) {
                    log.debug("Subject {} is not in required group: {}", subjectKey, requiredGroup);
                    throw new WebApplicationException(Response.Status.FORBIDDEN);
                }
            }
        }

        request.setAttribute(ATTRIBUTE_SUBJECT, subject);
        request.setAttribute(ATTRIBUTE_SUBJECT_KEY, subject.getKey());

        if (log.isTraceEnabled()) {
            log.trace("Using subjectKey={}", getCurrentSubjectKey());
        }

        return subjectKey;
    }

    /**
     * Returns the subject for the current request.
     * Returns null if authorize() has not been called for this request yet.
     */
    public Subject getCurrentSubject() {
        return (Subject)request.getAttribute(ATTRIBUTE_SUBJECT);
    }

    /**
     * Returns the subject key for the current request.
     * Returns null if authorize() has not been called for this request yet.
     */
    public Subject getCurrentSubjectKey() {
        return (Subject)request.getAttribute(ATTRIBUTE_SUBJECT);
    }
}

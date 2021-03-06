package org.janelia.jacs2.auth;

import org.janelia.model.security.Subject;
import org.janelia.model.security.User;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.SecurityContext;

public class JacsSecurityContextHelper {

    public static String getAuthenticatedSubjectKey(ContainerRequestContext containerRequestContext) {
        JacsSecurityContext jacsSecurityContext = getSecurityContext(containerRequestContext);
        return jacsSecurityContext == null ? null : jacsSecurityContext.getAuthenticatedSubjectKey();
    }

    public static String getAuthorizedSubjectKey(ContainerRequestContext containerRequestContext) {
        JacsSecurityContext jacsSecurityContext = getSecurityContext(containerRequestContext);
        return jacsSecurityContext == null ? null : jacsSecurityContext.getAuthorizedSubjectKey();
    }

    @SuppressWarnings("unchecked")
    public static <S extends Subject> S getAuthenticatedSubject(ContainerRequestContext containerRequestContext, Class<S> subjectType) {
        JacsSecurityContext jacsSecurityContext = getSecurityContext(containerRequestContext);
        return jacsSecurityContext == null ? null : (S) jacsSecurityContext.getAuthenticatedSubject();
    }

    @SuppressWarnings("unchecked")
    public static <S extends Subject> S getAuthorizedSubject(ContainerRequestContext containerRequestContext, Class<S> subjectType) {
        JacsSecurityContext jacsSecurityContext = getSecurityContext(containerRequestContext);
        return jacsSecurityContext == null ? null : (S) jacsSecurityContext.getAuthorizedSubject();
    }

    public static JacsSecurityContext getSecurityContext(ContainerRequestContext containerRequestContext) {
        SecurityContext securityContext = containerRequestContext.getSecurityContext();
        if (securityContext instanceof JacsSecurityContext) {
            return (JacsSecurityContext) containerRequestContext.getSecurityContext();
        } else {
            return JacsSecurityContext.UNAUTHENTICATED;
        }
    }
}

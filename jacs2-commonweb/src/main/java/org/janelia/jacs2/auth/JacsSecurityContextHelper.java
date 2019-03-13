package org.janelia.jacs2.auth;

import org.janelia.model.security.User;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.SecurityContext;

public class JacsSecurityContextHelper {

    public static String getAuthenticatedSubjectKey(ContainerRequestContext containerRequestContext) {
        JacsSecurityContext jacsSecurityContext = getSecurityContext(containerRequestContext);
        return jacsSecurityContext==null?null:jacsSecurityContext.getAuthenticatedSubject().getKey();
    }

    public static String getAuthorizedSubjectKey(ContainerRequestContext containerRequestContext) {
        JacsSecurityContext jacsSecurityContext = getSecurityContext(containerRequestContext);
        return jacsSecurityContext==null?null:jacsSecurityContext.getAuthorizedSubject().getKey();
    }

    public static User getAuthenticatedUser(ContainerRequestContext containerRequestContext) {
        JacsSecurityContext jacsSecurityContext = getSecurityContext(containerRequestContext);
        return jacsSecurityContext==null?null:(User)jacsSecurityContext.getAuthenticatedSubject();
    }

    public static User getAuthorizedUser(ContainerRequestContext containerRequestContext) {
        JacsSecurityContext jacsSecurityContext = getSecurityContext(containerRequestContext);
        return jacsSecurityContext==null?null:(User)jacsSecurityContext.getAuthorizedSubject();
    }

    private static JacsSecurityContext getSecurityContext(ContainerRequestContext containerRequestContext) {
        SecurityContext securityContext = containerRequestContext.getSecurityContext();
        if (securityContext instanceof JacsSecurityContext) {
            return (JacsSecurityContext) containerRequestContext.getSecurityContext();
        }
        return null;
    }
}

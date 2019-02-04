package org.janelia.jacs2.auth;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.SecurityContext;

public class JacsSecurityContextHelper {

    public static String getAuthenticatedSubjectKey(ContainerRequestContext containerRequestContext) {
        SecurityContext securityContext = containerRequestContext.getSecurityContext();
        if (securityContext instanceof JacsSecurityContext) {
            JacsSecurityContext jacsSecurityContext = (JacsSecurityContext) containerRequestContext.getSecurityContext();
            return jacsSecurityContext.getAuthenticatedSubject().getKey();
        } else {
            return null;
        }
    }

    public static String getAuthorizedSubjectKey(ContainerRequestContext containerRequestContext) {
        SecurityContext securityContext = containerRequestContext.getSecurityContext();
        if (securityContext instanceof JacsSecurityContext) {
            JacsSecurityContext jacsSecurityContext = (JacsSecurityContext) containerRequestContext.getSecurityContext();
            return jacsSecurityContext.getAuthorizedSubject().getKey();
        } else {
            return null;
        }
    }
}

package org.janelia.jacs2.auth;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.SecurityContext;

public class JacsSecurityContextHelper {

    public static String getAuthenticatedSubjectKey(ContainerRequestContext containerRequestContext) {
        JacsSecurityContext jacsSecurityContext = (JacsSecurityContext) containerRequestContext.getSecurityContext();
        return jacsSecurityContext.getAuthenticatedSubject().getKey();
    }

    public static String getAuthorizedSubjectKey(ContainerRequestContext containerRequestContext) {
        JacsSecurityContext jacsSecurityContext = (JacsSecurityContext) containerRequestContext.getSecurityContext();
        return jacsSecurityContext.getAuthorizedSubject().getKey();
    }
}

package org.janelia.jacs2.auth;

import javax.ws.rs.core.SecurityContext;

public class JacsSecurityContextHelper {

    public static String getAuthenticatedSubjectKey(SecurityContext securityContext) {
        JacsSecurityContext jacsSecurityContext = (JacsSecurityContext) securityContext;
        return jacsSecurityContext.getAuthenticatedSubject().getKey();
    }

    public static String getAuthorizedSubjectKey(SecurityContext securityContext) {
        JacsSecurityContext jacsSecurityContext = (JacsSecurityContext) securityContext;
        return jacsSecurityContext.getAuthorizedSubject().getKey();
    }
}

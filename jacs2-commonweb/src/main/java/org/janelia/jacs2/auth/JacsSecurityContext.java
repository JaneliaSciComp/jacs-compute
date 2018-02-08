package org.janelia.jacs2.auth;

import com.google.common.base.Preconditions;
import org.janelia.model.security.Subject;
import org.janelia.model.security.util.SubjectUtils;

import javax.ws.rs.core.SecurityContext;
import java.security.Principal;

public class JacsSecurityContext implements SecurityContext {

    private final Subject authenticatedSubject;
    private final Subject authorizedSubject;
    private final boolean secure;
    private final String authScheme;

    public JacsSecurityContext(Subject authenticatedSubject, Subject authorizedSubject, boolean secure, String authScheme) {
        Preconditions.checkArgument(authenticatedSubject != null, "Invalid authenticated subject");
        this.authenticatedSubject = authenticatedSubject;
        this.authorizedSubject = authorizedSubject != null ? authorizedSubject : authenticatedSubject;
        this.secure = secure;
        this.authScheme = authScheme;
    }

    @Override
    public Principal getUserPrincipal() {
        return () -> authorizedSubject.getKey();
    }

    @Override
    public boolean isUserInRole(String role) {
        return SubjectUtils.subjectIsInGroup(authorizedSubject, role);
    }

    @Override
    public boolean isSecure() {
        return secure;
    }

    @Override
    public String getAuthenticationScheme() {
        return authScheme;
    }

    public Subject getAuthenticatedSubject() {
        return authenticatedSubject;
    }

    public Subject getAuthorizedSubject() {
        return authorizedSubject;
    }
}

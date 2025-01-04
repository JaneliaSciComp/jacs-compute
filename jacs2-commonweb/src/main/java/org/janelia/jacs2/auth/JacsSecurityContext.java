package org.janelia.jacs2.auth;

import java.security.Principal;

import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.ext.Provider;

import com.google.common.base.Preconditions;
import org.janelia.model.security.Subject;
import org.janelia.model.security.util.SubjectUtils;

@Provider
public class JacsSecurityContext implements SecurityContext {

    static JacsSecurityContext UNAUTHENTICATED = new JacsSecurityContext();

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

    private JacsSecurityContext() {
        this.authenticatedSubject = null;
        this.authorizedSubject = null;
        this.secure = false;
        this.authScheme = null;
    }

    @Override
    public Principal getUserPrincipal() {
        return authorizedSubject::getKey;
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

    Subject getAuthenticatedSubject() {
        return authenticatedSubject;
    }

    public String getAuthenticatedSubjectKey() {
        return authenticatedSubject == null ? null : authenticatedSubject.getKey();
    }

    public boolean hasAdminPrivileges() {
        return SubjectUtils.isAdmin(authenticatedSubject);
    }

    Subject getAuthorizedSubject() {
        return authorizedSubject;
    }

    public String getAuthorizedSubjectKey() {
        return authorizedSubject == null ? null : authorizedSubject.getKey();
    }
}

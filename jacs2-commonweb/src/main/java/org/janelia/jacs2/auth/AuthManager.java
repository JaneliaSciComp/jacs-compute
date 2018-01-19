package org.janelia.jacs2.auth;

import org.janelia.model.security.Subject;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public interface AuthManager {

    String authorize(String... requiredGroups);

    Subject getCurrentSubject();

    String getCurrentSubjectKey();
}

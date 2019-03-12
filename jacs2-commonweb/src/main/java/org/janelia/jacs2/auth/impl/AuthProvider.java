package org.janelia.jacs2.auth.impl;

import org.janelia.model.security.User;

/**
 * Interface defining a method for authenticating users by username/password.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public interface AuthProvider {

    /**
     * Authenticate the given user and return the corresponding User object. If authentication fails,
     * then null is returned. System errors throw a RuntimeException.
     * @param username username
     * @param password plaintext password
     * @return persisted User object or null if user could not authenticate
     */
    User authenticate(String username, String password);

    /**
     * If possible, create the given user, populating the user metadata from an external source.
     * @param username username
     * @return persisted User object or null if user does not exist in the external source
     */
    User createUser(String username);
}

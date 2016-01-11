package org.janelia.workstation.jfs.security;

/**
 * Created by schauderd on 6/26/15.
 */
public interface Authorizer {
    boolean checkAccess(Token credentials);
}

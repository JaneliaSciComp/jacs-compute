package org.janelia.workstation.webdav;

/**
 * Created by schauderd on 6/26/15.
 */
public interface Authorizer {
    boolean checkAccess(Token credentials);
}

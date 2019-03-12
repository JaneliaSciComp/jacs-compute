package org.janelia.jacs2.rest.sync.v2.dataresources.dto;

/**
 * Standard request for authentication. This message matches the format of ldap-jwt, which is used for
 * LDAP/AD authentication.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class AuthenticationRequest {

    private String username;
    private String password;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}

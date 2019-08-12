package org.janelia.jacs2.rest.sync.v2.dataresources.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Standard response for authentication request. This message matches the format of ldap-jwt, which is used for
 * LDAP/AD authentication.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class AuthenticationResponse {

    @JsonProperty("user_name")
    private String username;
    private String token;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }
}

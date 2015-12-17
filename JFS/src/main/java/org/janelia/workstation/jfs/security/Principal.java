package org.janelia.workstation.jfs.security;

/**
 * Created by schauderd on 10/29/15.
 */
public class Principal {
    private String username;
    private Token userToken;
    private String[] groups;

    public Principal(String username, Token token) {
        this.userToken = token;
        this.username = username;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Token getUserToken() {
        return userToken;
    }

    public void setUserToken(Token userToken) {
        this.userToken = userToken;
    }

    public String[] getGroups() {
        return groups;
    }

    public void setGroups(String[] groups) {
        this.groups = groups;
    }
}

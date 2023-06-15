package org.janelia.jacs2.auth.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.directory.api.ldap.model.cursor.CursorException;
import org.apache.directory.api.ldap.model.cursor.EntryCursor;
import org.apache.directory.api.ldap.model.entry.Attribute;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.exception.LdapInvalidAttributeValueException;
import org.apache.directory.api.ldap.model.exception.LdapInvalidDnException;
import org.apache.directory.api.ldap.model.message.BindRequestImpl;
import org.apache.directory.api.ldap.model.message.BindResponse;
import org.apache.directory.api.ldap.model.message.ResultCodeEnum;
import org.apache.directory.api.ldap.model.message.SearchScope;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.ldap.client.api.*;
import org.janelia.model.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * LDAP-based authentication implementation.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class LDAPAuthProvider implements AuthProvider {

    private static final Logger LOG = LoggerFactory.getLogger(LDAPAuthProvider.class);
    private static final String USERNAME_PLACEHOLDER = "{{username}}";

    private final Dn baseDN;
    private final String searchFilter;
    private final LdapConnectionPool pool;

    private final String firstNameAttr;
    private final String lastNameAttr;
    private final String nameAttr;
    private final String emailAttr;
    private final String dnAttr;

    LDAPAuthProvider(String url, String searchBase, String searchFilter,
                     String bindDN, String bindCredentials, Integer timeout,
                     String firstNameAttr, String lastNameAttr,
                     String nameAttr, String emailAttr, String dnAttr) {

        this.searchFilter = searchFilter;
        this.firstNameAttr = firstNameAttr;
        this.lastNameAttr = lastNameAttr;
        this.nameAttr = nameAttr;
        this.emailAttr = emailAttr;
        this.dnAttr = dnAttr;

        try {
            this.baseDN = new Dn(searchBase);
        }
        catch (LdapInvalidDnException e) {
            throw new IllegalArgumentException(e);
        }

        URI ldapURI;
        if (StringUtils.startsWithIgnoreCase(url, "ldap://")
                || StringUtils.startsWithIgnoreCase(url, "ldaps://")) {
            ldapURI = URI.create(url);
        }
        else {
            ldapURI = URI.create(StringUtils.prependIfMissing(url,"ldap://"));
        }

        String server = ldapURI.getHost();
        int ldapPort = ldapURI.getPort();
        String scheme = ldapURI.getScheme();
        int port;
        boolean useSsl;
        if (StringUtils.equalsIgnoreCase(scheme, "ldaps")) {
            port = ldapPort <= 0 ? 636 : ldapPort; // use default ldaps port
            useSsl = true;
        }
        else {
            port = ldapPort <= 0 ? 389 : ldapPort; // use default ldap port
            useSsl = false;
        }

        LOG.info("Configuring LDAP authentication with the following parameters:");
        LOG.info("  URL: {}", url);
        LOG.info("  Server: {}", server);
        LOG.info("  Port: {}", port);
        LOG.info("  SSL: {}", useSsl);
        LOG.info("  Search base: {}", searchBase);
        LOG.info("  Search filter: {}", searchFilter);

        LdapConnectionConfig config = new LdapConnectionConfig();
        config.setLdapHost(server);
        config.setLdapPort(port);
        if (useSsl) {
            config.setUseSsl(true);
            config.setTrustManagers(new NoVerificationTrustManager());
        }
        if (timeout != null) {
            config.setTimeout(timeout);
        }

        if (!StringUtils.isAnyEmpty(bindDN, bindCredentials)) {
            LOG.info("  Bind name: {}", bindDN);
            config.setName(bindDN);
            config.setCredentials(bindCredentials);
        }

        this.pool = new LdapConnectionPool(new DefaultPoolableLdapConnectionFactory(config));
    }

    @Override
    public User authenticate(String username, String password) {
        LdapConnection connection = openLdapConnection();
        try {
            // DN resolution
            LdapUser ldapUser = getUserInfo(connection, username);
            if (ldapUser == null) {
                LOG.info("User not found in LDAP: {}", username);
                return null;
            }
            // Authenticate the user
            if (!authenticate(connection, ldapUser.getUserDn(), password)) {
                LOG.info("User failed authentication against LDAP: {}", username);
                return null;
            }
            return ldapUser.getUserInfo();
        }
        catch (LdapException e) {
            throw new RuntimeException(e);
        }
        finally {
            closeLdapConnection(connection);
        }
    }

    @Override
    public User generateUserInfo(String username) {
        LdapConnection connection = openLdapConnection();
        try {
            // DN resolution
            LdapUser ldapUser = getUserInfo(connection, username);
            if (ldapUser == null) {
                LOG.info("User not found in LDAP: {}", username);
                return null;
            }
            return ldapUser.getUserInfo();
        }
        catch (LdapException e) {
            throw new RuntimeException(e);
        }
        finally {
            closeLdapConnection(connection);
        }
    }

    /**
     * Get user information from LDAP.
     *
     * @param username username
     * @return User object populated with information from LDAP
     * @throws LdapException
     */
    private LdapUser getUserInfo(LdapConnection connection, String username) throws LdapException {
        EntryCursor cursor = null;
        try {
            LOG.debug("Get user info for {}", username);
            String filter = searchFilter.replace(USERNAME_PLACEHOLDER, username);
            cursor = connection.search(baseDN, filter, SearchScope.ONELEVEL);
            LdapUser ldapUser = null;
            while (cursor.next()) {
                if (ldapUser != null) {
                    LOG.warn("More than one LDAP user matches {}", filter);
                }
                Entry entry = cursor.get();
                User newUser = new User();
                String unameFromLDAPEntry = getOptionalLdapStringAttribute(entry, nameAttr);
                newUser.setName(StringUtils.defaultIfBlank(unameFromLDAPEntry, username));
                newUser.setKey("user:" + newUser.getName());
                newUser.setEmail(getOptionalLdapStringAttribute(entry, emailAttr));
                newUser.setFullName(entry.get(firstNameAttr).getString() + " " + entry.get(lastNameAttr).getString());
                ldapUser = new LdapUser(getLdapStringAttribute(entry, dnAttr), newUser);
            }

            return ldapUser;
        }
        catch (CursorException e) {
            throw new LdapException(e);
        }
        finally {
            if (cursor != null) closeLdapTraverseCursor(cursor);
        }
    }

    private String getLdapStringAttribute(Entry ldapEntry, String attributeName) {
        Attribute attr = ldapEntry.get(attributeName);
        if (attr != null) {
            try {
                return attr.getString();
            }
            catch (LdapInvalidAttributeValueException e) {
                throw new RuntimeException("Invalid value for attribute "+attributeName+" in "+ldapEntry, e);
            }
        }
        else {
            throw new RuntimeException("Attribute "+attributeName+" not found in "+ ldapEntry);
        }
    }

    private String getOptionalLdapStringAttribute(Entry ldapEntry, String attributeName) {
        Attribute attr = ldapEntry.get(attributeName);
        if (attr != null) {
            try {
                return attr.getString();
            }
            catch (LdapInvalidAttributeValueException e) {
                LOG.warn("Invalid value for attribute {} in {}", attributeName, ldapEntry, e);
                return null;
            }
        }
        else {
            LOG.warn("Attribute {} not found in {}", attributeName, ldapEntry);
            return null;
        }
    }

    private static class LdapUser {
        String userDn;
        User userInfo;

        public LdapUser(String userDn, User userInfo) {
            this.userDn = userDn;
            this.userInfo = userInfo;
        }

        public String getUserDn() {
            return userDn;
        }

        public User getUserInfo() {
            return userInfo;
        }
    }

    private boolean authenticate(LdapConnection connection, String userDn, String password) throws LdapException {
        LOG.trace("Re-binding with DN {} using password", userDn);

        if (StringUtils.isBlank(password)) {
            LOG.info("Empty password is not allowed for {}", userDn);
            return false;
        }

        final BindRequestImpl bindRequest = new BindRequestImpl();
        bindRequest.setName(userDn);
        bindRequest.setCredentials(password);

        final BindResponse bind = connection.bind(bindRequest);
        if (!bind.getLdapResult().getResultCode().equals(ResultCodeEnum.SUCCESS)) {
            LOG.info("Re-binding DN {} failed: {}", userDn, bind.getLdapResult().getResultCode().getMessage());
            return false;
        }

        LOG.trace("Binding DN {} successful, authenticated: {}", userDn, connection.isAuthenticated());
        return connection.isAuthenticated();
    }

    private LdapConnection openLdapConnection() {
        try {
            return pool.getConnection();
        }
        catch (LdapException e) {
            LOG.error("Error opening LDAP connection", e);
            throw new IllegalStateException(e);
        }
    }

    private void closeLdapConnection(LdapConnection conn) {
        try {
            conn.unBind();
        }
        catch (LdapException e) {
            LOG.error("Error unbinding the LDAP connection {}", conn, e);
        }
        try {
            pool.releaseConnection(conn);
        }
        catch (LdapException e) {
            LOG.error("Problems releasing LDAP connection {}", conn, e);
        }
    }

    private void closeLdapTraverseCursor(EntryCursor cursor) {
        try {
            cursor.close();
        }
        catch (IOException e) {
            LOG.error("Error closing LDAP cursor", e);
        }
    }
}

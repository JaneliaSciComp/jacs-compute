package org.janelia.jacs2.auth.impl;

import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.apache.directory.api.ldap.model.cursor.CursorException;
import org.apache.directory.api.ldap.model.cursor.EntryCursor;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.exception.LdapInvalidDnException;
import org.apache.directory.api.ldap.model.message.BindRequestImpl;
import org.apache.directory.api.ldap.model.message.BindResponse;
import org.apache.directory.api.ldap.model.message.ResultCodeEnum;
import org.apache.directory.api.ldap.model.message.SearchScope;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.ldap.client.api.DefaultPoolableLdapConnectionFactory;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapConnectionConfig;
import org.apache.directory.ldap.client.api.LdapConnectionPool;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.security.GroupRole;
import org.janelia.model.security.Subject;
import org.janelia.model.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * LDAP-based authentication implementation.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class LDAPAuthProvider implements AuthProvider {

    private static final Logger LOG = LoggerFactory.getLogger(LDAPAuthProvider.class);
    private static final String USERNAME_PLACEHOLDER = "{{username}}";

    private SubjectDao subjectDao;
    private final Dn baseDN;
    private final LdapConnectionPool pool;
    private final String searchFilter;

    LDAPAuthProvider(SubjectDao subjectDao, String url, String searchBase, String searchFilter,
                     String bindDN, String bindCredentials, Integer timeout) {
        this.subjectDao = subjectDao;
        LdapConnectionConfig config = new LdapConnectionConfig();
        try {
            this.baseDN = new Dn(searchBase);
            this.searchFilter = searchFilter;
        } catch (LdapInvalidDnException e) {
            throw new IllegalArgumentException(e);
        }

        int port = 389; // Default port
        String[] urlParts = url.replace("ldap://", "").split(":");
        String server = urlParts[0];
        if (urlParts.length == 2) {
            port = Integer.parseInt(urlParts[1]);
        }

        LOG.info("Configuring LDAP authentication with the following parameters:");
        LOG.info("  URL: {}", url);
        LOG.info("  Server: {}", server);
        LOG.info("  Port: {}", port);
        LOG.info("  Search base: {}", searchBase);
        LOG.info("  Search filter: {}", searchFilter);

        config.setLdapHost(server);
        config.setLdapPort(port);
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
            // Ensure user exists in Mongo
            return getPersistedUser(ldapUser.getUserInfo());
        } catch (LdapException e) {
            throw new RuntimeException(e);
        } finally {
            closeLdapConnection(connection);
        }
    }

    @Override
    public User createUser(String username) {
        LdapConnection connection = openLdapConnection();
        try {
            // DN resolution
            LdapUser ldapUser = getUserInfo(connection, username);
            if (ldapUser == null) {
                LOG.info("User not found in LDAP: {}", username);
                return null;
            }
            // Ensure user exists in Mongo
            return getPersistedUser(ldapUser.getUserInfo());
        } catch (LdapException e) {
            throw new RuntimeException(e);
        } finally {
            closeLdapConnection(connection);
        }
    }

    @Override
    public User addUser(Map<String, Object> userProperties) {
        // for now this is just a passthrough... we'll re-examine whether the logic needs to be different
        // in the future
        if (userProperties != null && userProperties.containsKey("name"))
            return createUser((String) userProperties.get("name"));
        return null;
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
                newUser.setEmail(entry.get("mail").getString());
                newUser.setFullName(entry.get("givenName").getString() + " " + entry.get("sn").getString());
                newUser.setUserGroupRole(Subject.USERS_KEY, GroupRole.Reader);
                newUser.setKey("user:" + username);
                newUser.setName(username);
                String userDN = entry.get("distinguishedname").getString();
                ldapUser = new LdapUser(userDN, newUser);
            }

            return ldapUser;
        } catch (CursorException e) {
            throw new LdapException(e);
        } finally {
            if (cursor != null) closeLdapTraverseCursor(cursor);
        }
    }

    private class LdapUser {
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

    /**
     * Takes unpersisted user info and checks to see if the same user already exists in the database. If so, the
     * persisted user is returned. Otherwise, the provided userInfo is persisted into the database, and the
     * persisted record is returned.
     *
     * @param userInfo unpersisted user populated with user's information
     * @return persisted User object
     */
    private User getPersistedUser(User userInfo) {
        Subject existingUser = subjectDao.findByKey(userInfo.getKey());
        if (existingUser != null) {
            return (User) existingUser;
        }
        subjectDao.save(userInfo);
        LOG.info("Created new user based on LDAP information: {}", userInfo);
        return userInfo;
    }

    private LdapConnection openLdapConnection() {
        try {
            return pool.getConnection();
        } catch (LdapException e) {
            LOG.error("Error opening LDAP connection", e);
            throw new IllegalStateException(e);
        }
    }

    private void closeLdapConnection(LdapConnection conn) {
        try {
            conn.unBind();
        } catch (LdapException e) {
            LOG.error("Error unbinding the LDAP connection {}", conn, e);
        }
        try {
            pool.releaseConnection(conn);
        } catch (LdapException e) {
            LOG.error("Problems releasing LDAP connection {}", conn, e);
        }
    }

    private void closeLdapTraverseCursor(EntryCursor cursor) {
        try {
            cursor.close();
        } catch (IOException e) {
            LOG.error("Error closing LDAP cursor", e);
        }
    }
}

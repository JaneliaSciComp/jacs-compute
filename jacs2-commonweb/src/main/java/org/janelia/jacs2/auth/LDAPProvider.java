package org.janelia.jacs2.auth;

import org.apache.commons.lang3.StringUtils;
import org.apache.directory.api.ldap.model.cursor.CursorException;
import org.apache.directory.api.ldap.model.cursor.EntryCursor;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.exception.LdapInvalidDnException;
import org.apache.directory.api.ldap.model.message.SearchScope;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.ldap.client.api.DefaultPoolableLdapConnectionFactory;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapConnectionConfig;
import org.apache.directory.ldap.client.api.LdapConnectionPool;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.security.GroupRole;
import org.janelia.model.security.Subject;
import org.janelia.model.security.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;

public class LDAPProvider {
    private static final Logger LOG = LoggerFactory.getLogger(LDAPProvider.class);

    private final Dn baseDN;
    private final LdapConnectionPool pool;

    @Inject
    public LDAPProvider(@PropertyValue(name = "LDAP.URL") String ldapUrl,
                        @PropertyValue(name = "LDAP.BaseDN") String ldapBaseDN) {
        LdapConnectionConfig config = new LdapConnectionConfig();
        try {
            this.baseDN = new Dn(ldapBaseDN);
        } catch (LdapInvalidDnException e) {
            throw new IllegalArgumentException(e);
        }
        config.setLdapHost(ldapUrl.replaceAll(":389", ""));
        config.setLdapPort(389);
        config.setTimeout(1000);
        this.pool = new LdapConnectionPool(new DefaultPoolableLdapConnectionFactory(config));
    }

    /**
     * Get user information from LDAP.
     * @param username
     * @return
     * @throws Exception
     */
    public User getUserInfo(String username) {
        User newUser = new User();
        LdapConnection connection = openLdapConnection();
        EntryCursor cursor;
        try {
           cursor = openLdapTraverseCursor(connection, username);
        } catch (LdapException e) {
            closeLdapConnection(connection);
            throw new IllegalStateException(e);
        }
        try {
            while (cursor.next()) {
                Entry entry = cursor.get();
                newUser.setEmail(entry.get("mail").getString());
                newUser.setFullName(entry.get("givenName").getString() + " " + entry.get("sn").getString());
                newUser.setUserGroupRole(Subject.USERS_KEY, GroupRole.Reader);
                newUser.setKey("user:" + username);
                newUser.setName(username);
            }
            return newUser;
        } catch (LdapException | CursorException e) {
            throw new IllegalStateException(e);
        } finally {
            closeLdapTraverseCursor(cursor);
            closeLdapConnection(connection);
        }
    }

    private LdapConnection openLdapConnection() {
        try {
            return pool.getConnection();
        } catch (LdapException e) {
            throw new IllegalStateException(e);
        }
    }

    private void closeLdapConnection(LdapConnection conn) {
        try {
            conn.unBind();
        } catch (LdapException ignore) {
            LOG.info("Error unbinding the LDAP connection {}", conn, ignore);
        }
        try {
            pool.releaseConnection(conn);
        } catch (LdapException ignore) {
            LOG.warn("Problems releasing LDAP connection {}", conn, ignore);
        }

    }

    private EntryCursor openLdapTraverseCursor(LdapConnection conn, String username) throws LdapException {
        return conn.search(baseDN, "(cn=" + username + ")", SearchScope.ONELEVEL);
    }

    private void closeLdapTraverseCursor(EntryCursor cursor) {
        try {
            cursor.close();
        } catch (IOException e) {
            LOG.info("Error closing LDAP cursor", e);
        }
    }

}

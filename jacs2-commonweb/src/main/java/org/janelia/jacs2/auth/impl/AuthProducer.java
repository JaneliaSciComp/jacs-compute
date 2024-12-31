package org.janelia.jacs2.auth.impl;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.auth.PasswordProvider;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Produces the correct authentication implementation based on user-defined properties.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@ApplicationScoped
public class AuthProducer {
    private static final Logger LOG = LoggerFactory.getLogger(AuthProducer.class);

    @Inject
    private SubjectDao subjectDao;
    @Inject
    private PasswordProvider pwProvider;

    @ApplicationScoped
    @Produces
    public AuthProvider getAuth(@PropertyValue(name = "LDAP.URL") String ldapUrl,
                                @PropertyValue(name = "LDAP.SearchBase") String ldapSearchBase,
                                @PropertyValue(name = "LDAP.SearchFilter") String ldapSearchFilter,
                                @PropertyValue(name = "LDAP.BindDN") String ldapBindDN,
                                @PropertyValue(name = "LDAP.BindCredentials") String ldapBindCredentials,
                                @PropertyValue(name = "LDAP.TimeOut") Integer ldapTimeout,
                                @PropertyValue(name = "LDAP.AttributeFirstName") String firstNameAttr,
                                @PropertyValue(name = "LDAP.AttributeLastName") String lastNameAttr,
                                @PropertyValue(name = "LDAP.AttributeName") String nameAttr,
                                @PropertyValue(name = "LDAP.AttributeEmail") String emailAttr,
                                @PropertyValue(name = "LDAP.AttributeDistinguishedName") String dnAttr
                                ) {
        if (StringUtils.isAnyEmpty(ldapUrl, ldapSearchBase, ldapSearchFilter)) {
            LOG.info("No LDAP configuration found. Defaulting to local auth implementation.");
            return new LocalAuthProvider(subjectDao, pwProvider);
        }
        return new LDAPAuthProvider(ldapUrl, ldapSearchBase,
                ldapSearchFilter, ldapBindDN, ldapBindCredentials, ldapTimeout,
                firstNameAttr, lastNameAttr, nameAttr, emailAttr, dnAttr);
    }
}

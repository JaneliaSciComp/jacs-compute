package org.janelia.jacs2.auth.impl;

import org.apache.commons.pool2.PooledObject;
import org.apache.directory.api.i18n.I18n;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.ldap.client.api.AbstractPoolableLdapConnectionFactory;
import org.apache.directory.ldap.client.api.DefaultLdapConnectionFactory;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.LdapConnectionConfig;
import org.apache.directory.ldap.client.api.LdapConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class PoolableLdapConnectionFactory extends AbstractPoolableLdapConnectionFactory
{
    /** This class logger */
    private static final Logger LOG = LoggerFactory.getLogger( AbstractPoolableLdapConnectionFactory.class );

    /**
     * Creates a new instance of PoolableLdapConnectionFactory.
     *
     * @param config the configuration for creating LdapConnections
     */
    public PoolableLdapConnectionFactory( LdapConnectionConfig config )
    {
        this( new DefaultLdapConnectionFactory( config ) );
    }


    /**
     * Creates a new instance of PoolableLdapConnectionFactory using an instance
     * of the supplied class as its LdapConnection factory.
     *
     * @param config the configuration for creating LdapConnections
     * @param connectionFactoryClass the class used as a factory for connections
     */
    public PoolableLdapConnectionFactory( LdapConnectionConfig config,
                                                 Class<? extends LdapConnectionFactory> connectionFactoryClass )
    {
        this( newLdapConnectionFactory( config, connectionFactoryClass ) );
    }


    /**
     * Creates a new instance of PoolableLdapConnectionFactory.
     *
     * @param connectionFactory the connection factory for creating LdapConnections
     */
    public PoolableLdapConnectionFactory( LdapConnectionFactory connectionFactory )
    {
        this.connectionFactory = connectionFactory;
    }

    /**
     * {@inheritDoc}
     *
     * There is nothing to do to activate a connection.
     */
    @Override
    public void activateObject( PooledObject<LdapConnection> connection ) throws LdapException
    {
        if ( LOG.isDebugEnabled() )
        {
            LOG.debug( I18n.msg( I18n.MSG_04146_ACTIVATING, connection ) );
        }

        if ( !connection.getObject().isConnected() || !connection.getObject().isAuthenticated() )
        {
            if ( LOG.isDebugEnabled() )
            {
                LOG.debug( I18n.msg( I18n.MSG_04147_REBIND_CONNECTION_DROPPED, connection ) );
            }

            connectionFactory.bindConnection( connection.getObject() );
        }
    }

}

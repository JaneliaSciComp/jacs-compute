package org.janelia.jacs2.cdi;

import com.google.common.base.Splitter;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.Jacs2Future;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.cdi.qualifier.Sage;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.model.access.dao.mongo.utils.RegistryHelper;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.sql.DataSource;
import java.util.List;
import java.util.stream.Collectors;

@ApplicationScoped
public class PersistenceProducer {

    @Inject
    private Logger log;

    @Inject
    @PropertyValue(name = "MongoDB.ServerName")
    private String mongoServer;

    @Inject
    @PropertyValue(name = "MongoDB.ConnectionURL")
    private String mongoConnectionURL;

    @Inject
    @PropertyValue(name = "MongoDB.Database")
    private String mongoDatabase;

    @Inject
    @PropertyValue(name = "MongoDB.AuthDatabase")
    private String authMongoDatabase;

    @Inject
    @PropertyValue(name = "MongoDB.FutureDatabase")
    private String mongoFutureDatabase;

    @ApplicationScoped
    @Produces
    public MongoClient createMongoClient(
            @PropertyValue(name = "MongoDB.ThreadsAllowedToBlockForConnectionMultiplier") int threadsAllowedToBlockMultiplier,
            @PropertyValue(name = "MongoDB.ConnectionsPerHost") int connectionsPerHost,
            @PropertyValue(name = "MongoDB.ConnectTimeout") int connectTimeout,
            @IntPropertyValue(name = "MongoDB.MaxWaitTimeInSecs", defaultValue = 120) int maxWaitTimeInSecs,
            @IntPropertyValue(name = "MongoDB.MaxConnectionIdleTimeInSecs") int maxConnectionIdleTimeInSecs,
            @IntPropertyValue(name = "MongoDB.MaxConnLifeTime") int maxConnLifeTimeInSecs,
            @PropertyValue(name = "MongoDB.Username") String username,
            @PropertyValue(name = "MongoDB.Password") String password,
            ObjectMapperFactory objectMapperFactory) {

        CodecRegistry codecRegistry = RegistryHelper.createCodecRegistry(objectMapperFactory);
        MongoClientOptions.Builder optionsBuilder =
                MongoClientOptions.builder()
                        .threadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockMultiplier)
                        .maxWaitTime(maxWaitTimeInSecs * 1000)
                        .maxConnectionIdleTime(maxConnectionIdleTimeInSecs * 1000)
                        .maxConnectionLifeTime(maxConnLifeTimeInSecs * 1000)
                        .connectionsPerHost(connectionsPerHost)
                        .connectTimeout(connectTimeout)
                        .codecRegistry(codecRegistry);

        if (StringUtils.isNotBlank(mongoServer)) {
            // Alternative connection method to support passwords special characters not supported by MongoClientURI
            List<ServerAddress> members = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(mongoServer)
                    .stream()
                    .map(ServerAddress::new)
                    .collect(Collectors.toList());
            if (StringUtils.isNotBlank(username)) {
                String credentialsDb = StringUtils.defaultIfBlank(authMongoDatabase, mongoDatabase);
                char[] passwordChars = StringUtils.isBlank(password) ? null : password.toCharArray();
                MongoCredential credential = MongoCredential.createCredential(username, credentialsDb, passwordChars);
                MongoClient m = new MongoClient(members, credential, optionsBuilder.build());
                log.info("Connected to MongoDB ({}@{}) as user {}", mongoDatabase, mongoServer, username);
                return m;
            } else {
                MongoClient m = new MongoClient(members, optionsBuilder.build());
                log.info("Connected to MongoDB ({}@{})", mongoDatabase, mongoServer);
                return m;
            }
        } else {
            // use connection URL
            if (StringUtils.isBlank(mongoConnectionURL)) {
                log.error("Neither mongo server(s) nor the mongo URL have been specified");
                throw new IllegalStateException("Neither mongo server(s) nor the mongo URL have been specified");
            } else {
                MongoClientURI mongoConnectionString = new MongoClientURI(mongoConnectionURL, optionsBuilder);
                MongoClient m = new MongoClient(mongoConnectionString);
                log.info("Connected to MongoDB ({}@{})", mongoDatabase, mongoConnectionString);
                return m;
            }
        }
    }

    @Produces
    @Default
    public MongoDatabase createDefaultMongoDatabase(MongoClient mongoClient) {
        log.trace("Creating default database: {}", mongoDatabase);
        return mongoClient.getDatabase(mongoDatabase);
    }

    @Produces
    @Jacs2Future
    public MongoDatabase createFutureMongoDatabase(MongoClient mongoClient) {
        log.info("Creating future database: {}", mongoFutureDatabase);
        return mongoClient.getDatabase(mongoFutureDatabase);
    }

    @ApplicationScoped
    @Produces
    public DataSource createDatasource(@PropertyValue(name = "mouselight.db.url") String dbUrl,
                                       @PropertyValue(name = "mouselight.db.user") String dbUser,
                                       @PropertyValue(name = "mouselight.db.password") String dbPassword,
                                       @StrPropertyValue(name = "mouselight.db.validationQuery", defaultValue = "select 1") String validationQuery,
                                       @IntPropertyValue(name = "mouselight.db.maxOpenedCursors", defaultValue = 20) int maxOpenedCursors) {
        return createPooledDatasource(dbUrl, dbUser, dbPassword, validationQuery, maxOpenedCursors);
    }

    @Sage
    @ApplicationScoped
    @Produces
    public DataSource createSageDatasource(@PropertyValue(name = "sage.db.url") String dbUrl,
                                           @PropertyValue(name = "sage.db.user") String dbUser,
                                           @PropertyValue(name = "sage.db.password") String dbPassword,
                                           @StrPropertyValue(name = "sage.db.validationQuery", defaultValue = "select 1") String validationQuery) {
        return createPooledDatasource(dbUrl, dbUser, dbPassword, validationQuery, 10);
    }

    private DataSource createPooledDatasource(String dbUrl, String dbUser, String dbPassword, String validationQuery, int maxOpenedCursors) {
        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(dbUrl, dbUser, dbPassword);
        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
        poolableConnectionFactory.setValidationQuery(StringUtils.defaultIfBlank(validationQuery, null));
        poolableConnectionFactory.setMaxOpenPreparedStatements(maxOpenedCursors);
        ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);
        poolableConnectionFactory.setPool(connectionPool);
        return new PoolingDataSource<>(connectionPool);
    }

}

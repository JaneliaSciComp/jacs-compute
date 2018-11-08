package org.janelia.jacs2.cdi;

import com.google.common.base.Splitter;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClients;
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
import org.janelia.model.access.dao.mongo.utils.RegistryHelper;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.sql.DataSource;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@ApplicationScoped
public class PersistenceProducer {

    @Inject
    private Logger log;

    @Inject
    @PropertyValue(name = "MongoDB.ServerURL")
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
        MongoClientSettings.Builder optionsBuilder =
                MongoClientSettings.builder()
                        .applyToSocketSettings(builder ->
                                builder.connectTimeout(connectTimeout, TimeUnit.MILLISECONDS))
                        .applyToConnectionPoolSettings(builder -> builder.maxSize(connectionsPerHost)
                                    .maxWaitQueueSize(threadsAllowedToBlockMultiplier * connectionsPerHost)
                                    .maxWaitTime(maxWaitTimeInSecs, TimeUnit.SECONDS)
                                    .maxConnectionIdleTime(maxConnectionIdleTimeInSecs, TimeUnit.SECONDS)
                                    .maxConnectionLifeTime(maxConnLifeTimeInSecs, TimeUnit.SECONDS)
                        )
                        .codecRegistry(codecRegistry);

        if (StringUtils.isNotBlank(mongoServer)) {
            // Alternative connection method to support passwords special characters not supported by MongoClientURI
            List<ServerAddress> members = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(mongoServer)
                    .stream()
                    .map(ServerAddress::new)
                    .collect(Collectors.toList());
            optionsBuilder.applyToClusterSettings(builder ->
                    builder.hosts(members));
            if (StringUtils.isNotBlank(username)) {
                String credentialsDb = StringUtils.defaultIfBlank(authMongoDatabase, mongoDatabase);
                char[] passwordChars = StringUtils.isBlank(password) ? null : password.toCharArray();
                MongoCredential credential = MongoCredential.createCredential(username, credentialsDb, passwordChars);
                optionsBuilder.credential(credential);
                MongoClient m = MongoClients.create(optionsBuilder.build());
                log.info("Connected to MongoDB ({}@{}) as user {}", mongoDatabase, mongoServer, username);
                return m;
            } else {
                MongoClient m = MongoClients.create(optionsBuilder.build());
                log.info("Connected to MongoDB ({}@{})", mongoDatabase, mongoServer);
                return m;
            }
        } else {
            // use the connection URI
            optionsBuilder.applyToServerSettings(builder -> builder.applyConnectionString(new ConnectionString(mongoConnectionURL)));
            MongoClient m = MongoClients.create(optionsBuilder.build());
            log.info("Connected to MongoDB ({}@{})", mongoDatabase, mongoConnectionURL);
            return m;
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

    @Sage
    @ApplicationScoped
    @Produces
    public DataSource createSageDatasource(@PropertyValue(name = "sage.db.url") String dbUrl,
                                           @PropertyValue(name = "sage.db.user") String dbUser,
                                           @PropertyValue(name = "sage.db.password") String dbPassword) {
        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(dbUrl, dbUser, dbPassword);
        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
        ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);
        poolableConnectionFactory.setPool(connectionPool);
        return new PoolingDataSource<>(connectionPool);
    }

}

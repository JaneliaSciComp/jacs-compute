package org.janelia.jacs2.cdi;

import com.google.common.base.Splitter;
import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.dbcp2.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.bson.codecs.configuration.CodecRegistry;
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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
            @PropertyValue(name = "MongoDB.Username") String username,
            @PropertyValue(name = "MongoDB.Password") String password,
            ObjectMapperFactory objectMapperFactory) {

        CodecRegistry codecRegistry = RegistryHelper.createCodecRegistry(objectMapperFactory);
        MongoClientOptions.Builder optionsBuilder =
                MongoClientOptions.builder()
                        .threadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockMultiplier)
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
                MongoCredential credential = MongoCredential.createMongoCRCredential(username, credentialsDb, passwordChars);
                MongoClient m = new MongoClient(members, credential, optionsBuilder.build());
                log.info("Connected to MongoDB ({}@{}) as user {}", mongoDatabase, mongoServer, username);
                return m;
            } else {
                MongoClient m = new MongoClient(members, optionsBuilder.build());
                log.info("Connected to MongoDB ({}@{})", mongoDatabase, mongoServer);
                return m;
            }
        } else {
            // use the connection URI
            MongoClientURI mongoConnectionString = new MongoClientURI(mongoConnectionURL, optionsBuilder);
            MongoClient m = new MongoClient(mongoConnectionString);
            log.info("Connected to MongoDB ({}@{})", mongoDatabase, mongoConnectionString);
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

    @ApplicationScoped
    @Produces
    public DataSource createDatasource(@PropertyValue(name = "mouselight.db.url") String dbUrl,
                                       @PropertyValue(name = "mouselight.db.user") String dbUser,
                                       @PropertyValue(name = "mouselight.db.password") String dbPassword) {
        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(dbUrl, dbUser, dbPassword);
        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
        ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);
        poolableConnectionFactory.setPool(connectionPool);
        return new PoolingDataSource<>(connectionPool);
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

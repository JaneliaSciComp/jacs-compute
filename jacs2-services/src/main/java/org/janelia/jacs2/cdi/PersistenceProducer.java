package org.janelia.jacs2.cdi;

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

@ApplicationScoped
public class PersistenceProducer {

    @Inject
    private Logger log;

    @Inject
    @PropertyValue(name = "MongoDB.ServerURL")
    private String mongoServerURL;

    @Inject
    @PropertyValue(name = "MongoDB.ConnectionURL")
    private String mongoConnectionURL;

    @Inject
    @PropertyValue(name = "MongoDB.Database")
    private String mongoDatabase;

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

        if (mongoServerURL != null) {
            // Alternative connection method to support passwords special characters not supported by MongoClientURI

            List<ServerAddress> members = new ArrayList<>();
            for (String serverMember : mongoServerURL.split(",")) {
                members.add(new ServerAddress(serverMember));
            }

            if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
                MongoCredential credential = MongoCredential.createMongoCRCredential(username, mongoDatabase, password.toCharArray());
                MongoClient m = new MongoClient(members, credential, optionsBuilder.build());
                log.info("Connected to MongoDB (" + mongoDatabase + "@" + mongoServerURL + ") as user " + username);
                return m;
            }
            else {
                MongoClient m = new MongoClient(members);
                log.info("Connected to MongoDB (" + mongoDatabase + "@" + mongoServerURL + ")");
                return m;
            }
        }

        MongoClientURI mongoConnectionString = new MongoClientURI(mongoConnectionURL, optionsBuilder);
        MongoClient m = new MongoClient(mongoConnectionString);
        log.info("Connected to MongoDB (" + mongoDatabase + "@" + mongoConnectionString + ")");
        return m;
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
                                           @PropertyValue(name = "sage.db.password") String dbPassword) throws SQLException {
        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(dbUrl, dbUser, dbPassword);
        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
        ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);
        poolableConnectionFactory.setPool(connectionPool);
        return new PoolingDataSource<>(connectionPool);
    }

}

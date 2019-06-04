package org.janelia.jacs2.cdi;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.sql.DataSource;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.Jacs2Future;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.cdi.qualifier.Sage;
import org.janelia.jacs2.cdi.qualifier.StrPropertyValue;
import org.janelia.model.access.domain.dao.mongo.mongodbutils.MongoDBHelper;
import org.janelia.model.access.domain.dao.mongo.mongodbutils.RegistryHelper;
import org.slf4j.Logger;

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
        return MongoDBHelper.createMongoClient(
                mongoConnectionURL,
                mongoServer,
                StringUtils.defaultIfBlank(authMongoDatabase, mongoDatabase),
                username,
                password,
                threadsAllowedToBlockMultiplier,
                connectionsPerHost,
                connectTimeout,
                maxWaitTimeInSecs,
                maxConnectionIdleTimeInSecs,
                maxConnLifeTimeInSecs,
                () -> RegistryHelper.createCodecRegistryWithJacsksonEncoder(objectMapperFactory.newMongoCompatibleObjectMapper())
        );
    }

    @ApplicationScoped
    @Produces
    public MongoDatabase createDefaultMongoDatabase(MongoClient mongoClient) {
        return MongoDBHelper.createMongoDatabase(mongoClient, mongoDatabase);
    }

    @Jacs2Future
    @ApplicationScoped
    @Produces
    public MongoDatabase createFutureMongoDatabase(MongoClient mongoClient) {
        log.info("Creating future database: {}", mongoFutureDatabase);
        return MongoDBHelper.createMongoDatabase(mongoClient, mongoFutureDatabase);
    }

    @ApplicationScoped
    @Produces
    public DataSource createDatasource(@PropertyValue(name = "mouselight.db.url") String dbUrl,
                                       @PropertyValue(name = "mouselight.db.user") String dbUser,
                                       @PropertyValue(name = "mouselight.db.password") String dbPassword,
                                       @StrPropertyValue(name = "mouselight.db.validationQuery", defaultValue = "select 1") String validationQuery) {
        return createPooledDatasource(dbUrl, dbUser, dbPassword, validationQuery);
    }

    @Sage
    @ApplicationScoped
    @Produces
    public DataSource createSageDatasource(@PropertyValue(name = "sage.db.url") String dbUrl,
                                           @PropertyValue(name = "sage.db.user") String dbUser,
                                           @PropertyValue(name = "sage.db.password") String dbPassword,
                                           @StrPropertyValue(name = "sage.db.validationQuery", defaultValue = "select 1") String validationQuery) {
        return createPooledDatasource(dbUrl, dbUser, dbPassword, validationQuery);
    }

    private DataSource createPooledDatasource(String dbUrl, String dbUser, String dbPassword, String validationQuery) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbUrl);
        config.setUsername(dbUser);
        config.setPassword(dbPassword);
        return new HikariDataSource(config);
    }

}

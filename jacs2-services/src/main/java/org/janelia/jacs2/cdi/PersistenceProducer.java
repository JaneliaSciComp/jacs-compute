package org.janelia.jacs2.cdi;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.cdi.qualifier.Sage;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.janelia.jacs2.model.jacsservice.ProcessingLocation;
import org.janelia.jacs2.dao.mongo.utils.BigIntegerCodec;
import org.janelia.jacs2.dao.mongo.utils.DomainCodecProvider;
import org.janelia.jacs2.dao.mongo.utils.EnumCodec;
import org.janelia.jacs2.dao.mongo.utils.MapOfEnumCodec;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class PersistenceProducer {

    @PropertyValue(name = "MongoDB.ConnectionURL")
    @Inject
    private String nmongoConnectionURL;
    @PropertyValue(name = "MongoDB.Database")
    @Inject
    private String mongoDatabase;

    @Singleton
    @Produces
    public MongoClient createMongoClient(ObjectMapperFactory objectMapperFactory) {
        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
                MongoClient.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(new DomainCodecProvider(objectMapperFactory)),
                CodecRegistries.fromCodecs(
                        new BigIntegerCodec(),
                        new EnumCodec<>(JacsServiceState.class),
                        new EnumCodec<>(ProcessingLocation.class),
                        new EnumCodec<>(FileType.class),
                        new MapOfEnumCodec<>(FileType.class, HashMap.class),
                        new MapOfEnumCodec<>(FileType.class, LinkedHashMap.class)
                )
        );
        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder().codecRegistry(codecRegistry);
        MongoClientURI mongoConnectionString = new MongoClientURI(nmongoConnectionURL, optionsBuilder);
        MongoClient mongoClient = new MongoClient(mongoConnectionString);
        return mongoClient;
    }

    @Produces
    public MongoDatabase createMongoDatabase(MongoClient mongoClient) {
        return mongoClient.getDatabase(mongoDatabase);
    }

    @Sage
    @Produces
    public Connection createSageConnection(@Sage Driver driver,
                                           @PropertyValue(name = "sage.db.url") String dbUrl,
                                           @PropertyValue(name = "sage.db.user") String dbUser,
                                           @PropertyValue(name = "sage.db.password") String dbPassword) throws SQLException {
        return DriverManager.getConnection(dbUrl, dbUser, dbPassword);
    }

    public void closeSageConnection(@Disposes @Sage Connection connection) {
        try {
            connection.close();
        } catch (SQLException ignore) {
        }
    }

    @Sage
    @ApplicationScoped
    @Produces
    public Driver createJdbcDriver(@PropertyValue(name = "sage.db.driver") String dbDriver) {
        try {
            Class<Driver> driverClass = (Class<Driver>) Class.forName(dbDriver);
            Driver driver = driverClass.newInstance();
            DriverManager.registerDriver(driver);
            return driver;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}

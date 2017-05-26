package org.janelia.jacs2.cdi;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.cdi.qualifier.Sage;
import org.janelia.jacs2.dao.mongo.utils.JacksonCodecProvider;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.janelia.jacs2.model.jacsservice.ProcessingLocation;
import org.janelia.jacs2.dao.mongo.utils.BigIntegerCodec;
import org.janelia.jacs2.dao.mongo.utils.DomainCodecProvider;
import org.janelia.jacs2.dao.mongo.utils.EnumCodec;
import org.janelia.jacs2.dao.mongo.utils.MapOfEnumCodec;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

@ApplicationScoped
public class PersistenceProducer {

    @PropertyValue(name = "MongoDB.ConnectionURL")
    @Inject
    private String nmongoConnectionURL;
    @PropertyValue(name = "MongoDB.Database")
    @Inject
    private String mongoDatabase;

    @ApplicationScoped
    @Produces
    public MongoClient createMongoClient(ObjectMapperFactory objectMapperFactory) {
        CodecRegistry codecRegistry = CodecRegistries.fromRegistries(
                MongoClient.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(new JacksonCodecProvider(objectMapperFactory)),
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

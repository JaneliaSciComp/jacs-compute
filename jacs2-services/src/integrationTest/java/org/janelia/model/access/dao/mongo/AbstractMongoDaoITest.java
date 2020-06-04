package org.janelia.model.access.dao.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.janelia.jacs2.AbstractITest;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.model.access.dao.ReadWriteDao;
import org.janelia.model.access.domain.IdGenerator;
import org.janelia.model.access.domain.TimebasedIdentifierGenerator;
import org.janelia.model.access.domain.dao.mongo.mongodbutils.MongoDBHelper;
import org.janelia.model.access.domain.dao.mongo.mongodbutils.RegistryHelper;
import org.janelia.model.jacs2.domain.interfaces.HasIdentifier;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Random;

public abstract class AbstractMongoDaoITest<T extends HasIdentifier> extends AbstractITest {
    private static MongoClient testMongoClient;
    protected static ObjectMapperFactory testObjectMapperFactory = ObjectMapperFactory.instance();

    protected MongoDatabase testMongoDatabase;
    protected IdGenerator<Long> idGenerator = new TimebasedIdentifierGenerator(0);
    protected Random dataGenerator = new Random();

    @BeforeClass
    public static void setUpMongoClient() {
        testMongoClient = MongoDBHelper.createMongoClient(
                integrationTestsConfig.getStringPropertyValue("MongoDB.ConnectionURL"),
                integrationTestsConfig.getStringPropertyValue("MongoDB.ServerName"),
                integrationTestsConfig.getStringPropertyValue("MongoDB.AuthDatabase", integrationTestsConfig.getStringPropertyValue("MongoDB.Database")),
                integrationTestsConfig.getStringPropertyValue("MongoDB.Username"),
                integrationTestsConfig.getStringPropertyValue("MongoDB.Password"),
                0, // use default
                0, // use default
                -1, // use default
                0,
                0,
                0,
                () -> RegistryHelper.createCodecRegistryWithJacsksonEncoder(testObjectMapperFactory.newMongoCompatibleObjectMapper())
        );
    }

    @Before
    public final void setUpMongoDatabase() {
        testMongoDatabase = MongoDBHelper.createMongoDatabase(testMongoClient, integrationTestsConfig.getStringPropertyValue("MongoDB.Database"));
    }

    protected void deleteAll(ReadWriteDao<T, Number> dao, List<T> es) {
        for (T e : es) {
            delete(dao, e);
        }
    }

    protected void delete(ReadWriteDao<T, Number> dao, T e) {
        if (e.getId() != null) {
            dao.delete(e);
        }
    }

    protected abstract List<T> createMultipleTestItems(int nItems);

}

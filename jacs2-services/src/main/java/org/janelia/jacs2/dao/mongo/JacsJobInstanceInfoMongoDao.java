package org.janelia.jacs2.dao.mongo;

import com.mongodb.client.MongoDatabase;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.jacs2.dao.JacsJobInstanceInfoDao;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.model.jacsservice.JacsJobInstanceInfo;

import javax.inject.Inject;

/**
 * Mongo based implementation of JacsJobInstanceInfoDao.
 */
public class JacsJobInstanceInfoMongoDao extends AbstractMongoDao<JacsJobInstanceInfo> implements JacsJobInstanceInfoDao {

    @Inject
    public JacsJobInstanceInfoMongoDao(MongoDatabase mongoDatabase, @JacsDefault TimebasedIdentifierGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }
}

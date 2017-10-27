package org.janelia.model.access.dao.mongo;

import com.mongodb.client.MongoDatabase;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.access.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.model.service.JacsJobInstanceInfo;

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

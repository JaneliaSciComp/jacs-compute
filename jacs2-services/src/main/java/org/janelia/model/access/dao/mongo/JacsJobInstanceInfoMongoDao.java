package org.janelia.model.access.dao.mongo;

import com.mongodb.client.MongoDatabase;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.access.domain.IdGenerator;
import org.janelia.model.service.JacsJobInstanceInfo;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Mongo based implementation of JacsJobInstanceInfoDao.
 */
@ApplicationScoped
public class JacsJobInstanceInfoMongoDao extends AbstractMongoDao<JacsJobInstanceInfo> implements JacsJobInstanceInfoDao {

    @Inject
    public JacsJobInstanceInfoMongoDao(MongoDatabase mongoDatabase, @JacsDefault IdGenerator<Long> idGenerator) {
        super(mongoDatabase, idGenerator);
    }
}

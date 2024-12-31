package org.janelia.model.access.dao.mongo;

import com.mongodb.client.MongoDatabase;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.model.access.dao.JacsNotificationDao;
import org.janelia.model.access.domain.IdGenerator;
import org.janelia.model.service.JacsNotification;

import jakarta.inject.Inject;

/**
 * Mongo based implementation of JacsNotificationDao.
 */
public class JacsNotificationMongoDao extends AbstractMongoDao<JacsNotification> implements JacsNotificationDao {

    @Inject
    public JacsNotificationMongoDao(MongoDatabase mongoDatabase, @JacsDefault IdGenerator<Long> idGenerator) {
        super(mongoDatabase, idGenerator);
    }
}

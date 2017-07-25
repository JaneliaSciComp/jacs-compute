package org.janelia.jacs2.dao.mongo;

import com.mongodb.client.MongoDatabase;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.jacs2.dao.JacsNotificationDao;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.model.jacsservice.JacsNotification;

import javax.inject.Inject;

/**
 * Mongo based implementation of JacsNotificationDao.
 */
public class JacsNotificationMongoDao extends AbstractMongoDao<JacsNotification> implements JacsNotificationDao {

    @Inject
    public JacsNotificationMongoDao(MongoDatabase mongoDatabase, @JacsDefault TimebasedIdentifierGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }
}

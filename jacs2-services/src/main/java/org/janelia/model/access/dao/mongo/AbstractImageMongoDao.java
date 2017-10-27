package org.janelia.model.access.dao.mongo;

import com.mongodb.client.MongoDatabase;
import org.janelia.model.jacs2.dao.ImageDao;
import org.janelia.model.jacs2.domain.sample.Image;
import org.janelia.model.access.dao.mongo.utils.TimebasedIdentifierGenerator;

public abstract class AbstractImageMongoDao<T extends Image> extends AbstractDomainObjectDao<T> implements ImageDao<T> {
    public AbstractImageMongoDao(MongoDatabase mongoDatabase, TimebasedIdentifierGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }
}

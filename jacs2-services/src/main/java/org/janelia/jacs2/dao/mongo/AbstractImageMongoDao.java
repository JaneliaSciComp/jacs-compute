package org.janelia.jacs2.dao.mongo;

import com.mongodb.client.MongoDatabase;
import org.janelia.jacs2.dao.ImageDao;
import org.janelia.it.jacs.model.domain.sample.Image;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;

public abstract class AbstractImageMongoDao<T extends Image> extends AbstractDomainObjectDao<T> implements ImageDao<T> {
    public AbstractImageMongoDao(MongoDatabase mongoDatabase, TimebasedIdentifierGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }
}

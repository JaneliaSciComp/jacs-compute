package org.janelia.model.jacs2.dao.mongo;

import com.mongodb.client.MongoDatabase;

import org.janelia.model.access.domain.IdGenerator;
import org.janelia.model.jacs2.dao.ImageDao;
import org.janelia.model.jacs2.domain.sample.Image;

public abstract class AbstractImageMongoDao<T extends Image> extends AbstractDomainObjectDao<T> implements ImageDao<T> {
    public AbstractImageMongoDao(MongoDatabase mongoDatabase, IdGenerator<Long> idGenerator) {
        super(mongoDatabase, idGenerator);
    }
}

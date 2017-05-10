package org.janelia.jacs2.dao.mongo;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.dao.ImageDao;
import org.janelia.it.jacs.model.domain.sample.Image;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;

import static com.mongodb.client.model.Filters.eq;

public abstract class AbstractImageMongoDao<T extends Image> extends AbstractDomainObjectDao<T> implements ImageDao<T> {
    public AbstractImageMongoDao(MongoDatabase mongoDatabase, TimebasedIdentifierGenerator idGenerator, ObjectMapperFactory objectMapperFactory) {
        super(mongoDatabase, idGenerator, objectMapperFactory);
    }

    @Override
    public void updateImageFiles(Image image) {
        mongoCollection.updateOne(eq("_id", image.getId()), Updates.set("files", image.getFiles()));
    }
}

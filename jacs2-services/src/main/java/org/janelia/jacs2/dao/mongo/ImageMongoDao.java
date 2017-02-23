package org.janelia.jacs2.dao.mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.dao.ImageDao;
import org.janelia.it.jacs.model.domain.sample.Image;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;

import javax.inject.Inject;

import static com.mongodb.client.model.Filters.eq;

public class ImageMongoDao extends AbstractDomainObjectDao<Image> implements ImageDao {
    @Inject
    public ImageMongoDao(MongoDatabase mongoDatabase, TimebasedIdentifierGenerator idGenerator, ObjectMapperFactory objectMapperFactory) {
        super(mongoDatabase, idGenerator, objectMapperFactory);
    }

    @Override
    public void updateImageFiles(Image image) {
        mongoCollection.updateOne(eq("_id", image.getId()), Updates.set("files", image.getFiles()));
    }
}

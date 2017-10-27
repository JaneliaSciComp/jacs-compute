package org.janelia.model.jacs2.dao.mongo;

import com.mongodb.client.MongoDatabase;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.cdi.qualifier.Jacs2Future;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.model.access.dao.mongo.AbstractMongoDao;
import org.janelia.model.jacs2.dao.SubjectDao;
import org.janelia.model.jacs2.domain.Subject;
import org.janelia.model.access.dao.mongo.utils.TimebasedIdentifierGenerator;

import javax.inject.Inject;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

public class SubjectMongoDao extends AbstractMongoDao<Subject> implements SubjectDao {
    @Inject
    @Jacs2Future
    public SubjectMongoDao(MongoDatabase mongoDatabase, @JacsDefault TimebasedIdentifierGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }

    @Override
    public Subject findByName(String subjectName) {
        List<Subject> entityDocs = find(eq("name", subjectName), null, 0, 1, Subject.class);
        return CollectionUtils.isEmpty(entityDocs) ? null : entityDocs.get(0);
    }
}

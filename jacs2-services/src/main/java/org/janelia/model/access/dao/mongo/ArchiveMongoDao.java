package org.janelia.model.access.dao.mongo;

import com.google.common.base.Preconditions;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.model.access.dao.ArchiveDao;
import org.janelia.model.jacs2.DomainModelUtils;
import org.janelia.model.jacs2.domain.interfaces.HasIdentifier;
import org.janelia.model.jacs2.domain.support.MongoMapping;

import javax.inject.Inject;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

/**
 * Archive Mongo DAO implementation.
 *
 * @param <T> type of the element
 */
public class ArchiveMongoDao<T extends HasIdentifier> implements ArchiveDao<T, Number> {

    private final MongoCollection<T> activeMongoCollection;
    final MongoCollection<T> archiveMongoCollection;

    @Inject
    public ArchiveMongoDao(MongoDatabase mongoDatabase) {
        Pair<String, String> entityCollectionNames = getDomainObjectCollectionNames();
        Class<T> entityType = getEntityType();
        activeMongoCollection = mongoDatabase.getCollection(entityCollectionNames.getLeft(), entityType);
        if (StringUtils.isNotEmpty(entityCollectionNames.getRight())) {
            archiveMongoCollection = mongoDatabase.getCollection(entityCollectionNames.getRight(), entityType);
        } else {
            throw new IllegalArgumentException(entityType + " does not support archiving");
        }
    }

    private Class<T> getEntityType() {
        return DomainModelUtils.getGenericParameterType(this.getClass(), 0);
    }

    private Pair<String, String> getDomainObjectCollectionNames() {
        Class<T> entityClass = getEntityType();
        MongoMapping mongoMapping = DomainModelUtils.getMapping(entityClass);
        Preconditions.checkArgument(mongoMapping != null, "Entity class " + entityClass.getName() + " is not annotated with MongoMapping");
        return ImmutablePair.of(mongoMapping.collectionName(), mongoMapping.archiveCollectionName());
    }

    @Override
    public T findArchivedEntityById(Number id) {
        List<T> entityDocs = MongoDaoHelper.find(eq("_id", id), null, 0, 2, archiveMongoCollection, getEntityType());
        return CollectionUtils.isEmpty(entityDocs) ? null : entityDocs.get(0);
    }

    @Override
    public void archive(T entity) {
        if (archiveMongoCollection == null) {
            throw new UnsupportedOperationException("Archive is not supported for " + getEntityType());
        }
        archiveMongoCollection.insertOne(entity);
        MongoDaoHelper.delete(activeMongoCollection, entity.getId());
    }
}

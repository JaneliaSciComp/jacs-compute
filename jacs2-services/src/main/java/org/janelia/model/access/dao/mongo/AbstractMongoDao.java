package org.janelia.model.access.dao.mongo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.conversions.Bson;
import org.janelia.model.access.dao.AbstractDao;
import org.janelia.model.access.dao.DaoUpdateResult;
import org.janelia.model.access.dao.ReadWriteDao;
import org.janelia.model.access.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.model.jacs2.AppendFieldValueHandler;
import org.janelia.model.jacs2.DomainModelUtils;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.domain.interfaces.HasIdentifier;
import org.janelia.model.jacs2.domain.support.MongoMapping;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;

/**
 * Abstract Mongo DAO.
 *
 * @param <T> type of the element
 */
public abstract class AbstractMongoDao<T extends HasIdentifier> extends AbstractDao<T, Number> implements ReadWriteDao<T, Number> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMongoDao.class);

    protected final TimebasedIdentifierGenerator idGenerator;
    final MongoCollection<T> mongoCollection;

    protected AbstractMongoDao(MongoDatabase mongoDatabase, TimebasedIdentifierGenerator idGenerator) {
        Class<T> entityClass = getEntityType();
        String entityCollectionName = getDomainObjectCollectionName(entityClass);
        mongoCollection = mongoDatabase.getCollection(entityCollectionName, entityClass);
        this.idGenerator = idGenerator;
    }

    private String getDomainObjectCollectionName(Class<T> entityClass) {
        MongoMapping mongoMapping = DomainModelUtils.getMapping(entityClass);
        Preconditions.checkArgument(mongoMapping != null, "Entity class " + entityClass.getName() + " is not annotated with MongoMapping");
        return mongoMapping.collectionName();
    }

    @Override
    public T findById(Number id) {
        return MongoDaoHelper.findById(id, mongoCollection, getEntityType());
    }

    @Override
    public List<T> findByIds(Collection<Number> ids) {
        return MongoDaoHelper.findByIds(ids, mongoCollection, getEntityType());
    }

    @Override
    public PageResult<T> findAll(PageRequest pageRequest) {
        List<T> results = find(
                null,
                MongoDaoHelper.createBsonSortCriteria(pageRequest.getSortCriteria()),
                pageRequest.getOffset(),
                pageRequest.getPageSize(),
                getEntityType());
        return new PageResult<>(pageRequest, results);
    }

    protected <R> List<R> find(Bson queryFilter, Bson sortCriteria, long offset, int length, Class<R> resultType) {
        return MongoDaoHelper.find(queryFilter, sortCriteria, offset, length, mongoCollection, resultType);
    }

    @Override
    public long countAll() {
        return mongoCollection.count();
    }

    @Override
    public void save(T entity) {
        if (entity.getId() == null) {
            entity.setId(idGenerator.generateId());
            mongoCollection.insertOne(entity);
        }
        else {
            mongoCollection.replaceOne(getUpdateMatchCriteria(entity), entity);
        }
    }

    @Override
    public void saveAll(Collection<T> entities) {
        Iterator<Number> idIterator = idGenerator.generateIdList(entities.size()).iterator();
        List<T> toInsert = new ArrayList<>();
        entities.forEach(e -> {
            if (e.getId() == null) {
                e.setId(idIterator.next());
                toInsert.add(e);
            }
        });
        if (!toInsert.isEmpty()) {
            mongoCollection.insertMany(toInsert);
        }
    }

    @Override
    public DaoUpdateResult update(T entity, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(false);
        return update(entity, fieldsToUpdate, updateOptions);
    }

    private DaoUpdateResult update(T entity, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate, UpdateOptions updateOptions) {
        return update(getUpdateMatchCriteria(entity), getUpdates(fieldsToUpdate), updateOptions);
    }

    protected DaoUpdateResult update(Bson query, Bson toUpdate, UpdateOptions updateOptions) {
        LOG.trace("Update: {} -> {}", query, toUpdate);
        UpdateResult result = mongoCollection.updateOne(query, toUpdate, updateOptions);
        return new DaoUpdateResult(result.getMatchedCount(), result.getModifiedCount());
    }

    protected Bson getUpdates(Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        List<Bson> fieldUpdates = fieldsToUpdate.entrySet().stream()
                .map(e -> getFieldUpdate(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
        return Updates.combine(fieldUpdates);
    }

    protected Bson getUpdateMatchCriteria(T entity) {
        return eq("_id", entity.getId());
    }

    @Override
    public void delete(T entity) {
        MongoDaoHelper.delete(mongoCollection, entity.getId());
    }

    @SuppressWarnings("unchecked")
    private Bson getFieldUpdate(String fieldName, EntityFieldValueHandler<?> valueHandler) {
        if (valueHandler == null || valueHandler.getFieldValue() == null) {
            return Updates.unset(fieldName);
        } else if (valueHandler instanceof AppendFieldValueHandler) {
            Object value = valueHandler.getFieldValue();
            if (value instanceof Iterable) {
                if (Set.class.isAssignableFrom(value.getClass())) {
                    return Updates.addEachToSet(fieldName, ImmutableList.copyOf((Iterable) value));
                } else {
                    return Updates.pushEach(fieldName, ImmutableList.copyOf((Iterable) value));
                }
            } else {
                return Updates.push(fieldName, value);
            }
        } else {
            return Updates.set(fieldName, valueHandler.getFieldValue());
        }
    }
}

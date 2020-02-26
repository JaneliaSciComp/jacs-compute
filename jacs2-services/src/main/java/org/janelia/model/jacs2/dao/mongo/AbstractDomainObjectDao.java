package org.janelia.model.jacs2.dao.mongo;

import com.google.common.base.Preconditions;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.janelia.model.access.dao.DaoUpdateResult;
import org.janelia.model.access.dao.mongo.AbstractMongoDao;
import org.janelia.model.access.dao.mongo.MongoDaoHelper;
import org.janelia.model.jacs2.dao.DomainObjectDao;
import org.janelia.model.jacs2.domain.DomainObject;
import org.janelia.model.jacs2.domain.Subject;
import org.janelia.model.util.TimebasedIdentifierGenerator;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.jacs2.DomainModelUtils;

import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;

/**
 * Abstract Domain object DAO.
 *
 * @param <T> type of the element
 */
public abstract class AbstractDomainObjectDao<T extends DomainObject> extends AbstractMongoDao<T> implements DomainObjectDao<T> {

    public AbstractDomainObjectDao(MongoDatabase mongoDatabase, TimebasedIdentifierGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }

    @Override
    public PageResult<T> findByOwnerKey(Subject subject, String ownerKey, PageRequest pageRequest) {
        List<T> results = find(eq("ownerKey", ownerKey),
                MongoDaoHelper.createBsonSortCriteria(pageRequest.getSortCriteria()),
                pageRequest.getOffset(),
                pageRequest.getPageSize(),
                getEntityType());
        return new PageResult<>(pageRequest, results);
    }

    @Override
    public <U extends T> List<U> findSubtypesByIds(Subject subject, List<Number> ids, Class<U> entityType) {
        if (CollectionUtils.isEmpty(ids)) {
            return Collections.emptyList();
        } else if (DomainModelUtils.isAdminOrUndefined(subject)) {
            return find(
                    Filters.in("_id", ids),
                    null,
                    0,
                    -1,
                    entityType);
        } else {
            return find(
                    Filters.and(
                            createSubjectReadPermissionFilter(subject),
                            Filters.in("_id", ids)
                    ),
                    null,
                    0,
                    -1,
                    entityType);
        }
    }

    @Override
    public DaoUpdateResult update(T entity, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        Map<String, EntityFieldValueHandler<?>> fieldsWithUpdatedDate = new LinkedHashMap<>(fieldsToUpdate);
        entity.setUpdatedDate(new Date());
        fieldsWithUpdatedDate.put("updatedDate", new SetFieldValueHandler<>(entity.getUpdatedDate()));
        return super.update(entity, fieldsWithUpdatedDate);
    }

    @Override
    public boolean lockEntity(String lockKey, T entity) {
        Preconditions.checkArgument(StringUtils.isNotBlank(lockKey));
        Bson lockedEntity = Updates.combine(Updates.set("lockKey", lockKey), Updates.set("lockTimestamp", new Date()));
        entity.setLockKey(lockKey);
        DaoUpdateResult updateResult = update(getUpdateMatchCriteria(entity), lockedEntity, new UpdateOptions());
        return updateResult.getEntitiesFound() > 0 && updateResult.getEntitiesAffected() > 0;
    }

    @Override
    public boolean unlockEntity(String lockKey, T entity) {
        Bson lockedEntity = Updates.combine(Updates.unset("lockKey"), Updates.unset("lockTimestamp"));
        entity.setLockKey(lockKey);
        DaoUpdateResult updateResult = update(getUpdateMatchCriteria(entity), lockedEntity, new UpdateOptions());
        return updateResult.getEntitiesFound() > 0 && updateResult.getEntitiesAffected() > 0;
    }

    protected Bson getUpdateMatchCriteria(T entity) {
        Bson lockFilter;
        if (StringUtils.isNotBlank(entity.getLockKey())) {
            lockFilter = Filters.or(Filters.exists("lockKey", false), Filters.eq("lockKey", null), eq("lockKey", entity.getLockKey()));
        } else {
            lockFilter = Filters.or(Filters.exists("lockKey", false), Filters.eq("lockKey", null));
        }
        Bson subjectFilter;
        if (StringUtils.isBlank(entity.getOwnerName())) {
            // either there's no owner key or the writers contain group:all
            subjectFilter = Filters.or(Filters.exists("ownerKey", false), Filters.elemMatch("writers", eq("group:all")));
        } else {
            // use the owner key to check for write permissions
            Subject subject = new Subject();
            subject.setKey(entity.getOwnerKey());
            subjectFilter = createSubjectWritePermissionFilter(subject);
        }
        return Filters.and(
                eq("_id", entity.getId()),
                lockFilter,
                subjectFilter
        );
    }

    @Override
    public List<T> findByIds(Subject subject, List<Number> ids) {
        return findSubtypesByIds(subject, ids, getEntityType());
    }

    protected Bson createSubjectReadPermissionFilter(Subject subject) {
        return Filters.or(
                Filters.eq("ownerKey", subject.getKey()),
                Filters.in("readers", subject.getSubjectClaims())
        );
    }

    protected Bson createSubjectWritePermissionFilter(Subject subject) {
        return Filters.or(
                Filters.eq("ownerKey", subject.getKey()),
                Filters.in("writers", subject.getSubjectClaims())
        );
    }

}

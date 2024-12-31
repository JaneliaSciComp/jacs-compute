package org.janelia.model.access.dao.mongo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.janelia.jacs2.cdi.qualifier.BoolPropertyValue;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.model.access.dao.DaoUpdateResult;
import org.janelia.model.access.dao.JacsServiceDataDao;
import org.janelia.model.access.domain.IdGenerator;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.jacs2.page.SortCriteria;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;

import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;

/**
 * Mongo based implementation of JacsServiceDataDao.
 */
public class JacsServiceDataMongoDao extends AbstractMongoDao<JacsServiceData> implements JacsServiceDataDao {

    @Inject
    public JacsServiceDataMongoDao(MongoDatabase mongoDatabase,
                                   @JacsDefault IdGenerator<Long> idGenerator,
                                   @BoolPropertyValue(name = "MongoDB.createCollectionIndexes") boolean createCollectionIndexes) {
        super(mongoDatabase, idGenerator);
        if (createCollectionIndexes) {
            mongoCollection.createIndexes(
                    ImmutableList.of(
                            new IndexModel(Indexes.ascending("name")),
                            new IndexModel(Indexes.ascending("ownerKey")),
                            new IndexModel(Indexes.ascending("parentServiceId")),
                            new IndexModel(Indexes.ascending("rootServiceId")),
                            new IndexModel(Indexes.ascending("queueId")),
                            new IndexModel(Indexes.ascending("state")),
                            new IndexModel(Indexes.ascending("creationDate"))
                    )
            );
        }
    }

    @Override
    public List<JacsServiceData> findChildServices(Number serviceId) {
        return MongoDaoHelper.find(eq("parentServiceId", serviceId), null, 0, -1, mongoCollection, JacsServiceData.class);
    }

    @Override
    public JacsServiceData findServiceHierarchy(Number serviceId) {
        JacsServiceData jacsServiceData = findById(serviceId);
        if (jacsServiceData == null) {
            return null;
        }
        Number rootServiceId;
        if (jacsServiceData.getRootServiceId() == null) {
            rootServiceId = jacsServiceData.getId();
        } else {
            rootServiceId = jacsServiceData.getRootServiceId();
        }
        Map<Number, JacsServiceData> fullServiceHierachy = new LinkedHashMap<>();
        MongoDaoHelper.find(
                Filters.or(eq("rootServiceId", rootServiceId), eq("_id", rootServiceId)),
                MongoDaoHelper.createBsonSortCriteria(ImmutableList.of(new SortCriteria("_id"))),
                0,
                -1,
                mongoCollection,
                JacsServiceData.class)
                .forEach(sd -> {
                    fullServiceHierachy.put(sd.getId(), sd);
                });
        fullServiceHierachy.forEach((k, sd) -> {
            JacsServiceData parentService = fullServiceHierachy.get(sd.getParentServiceId());
            if (parentService != null) {
                sd.updateParentService(parentService);
            }
        });
        fullServiceHierachy.forEach((k, sd) -> {
            sd.getDependenciesIds()
                    .stream()
                    .filter(fullServiceHierachy::containsKey)
                    .forEach(id -> sd.addServiceDependency(fullServiceHierachy.get(id)));
        });
        return fullServiceHierachy.get(jacsServiceData.getId());
    }

    @Override
    public long countMatchingServices(JacsServiceData pattern, DataInterval<Date> creationInterval) {
        Bson bsonFilter = JacsServiceDataMongoHelper.createBsonMatchingFilter(pattern, creationInterval);
        return MongoDaoHelper.count(bsonFilter, mongoCollection);
    }

    @Override
    public PageResult<JacsServiceData> findMatchingServices(JacsServiceData pattern, DataInterval<Date> creationInterval, PageRequest pageRequest) {
        Bson bsonFilter = JacsServiceDataMongoHelper.createBsonMatchingFilter(pattern, creationInterval);
        List<JacsServiceData> results = MongoDaoHelper.find(bsonFilter, MongoDaoHelper.createBsonSortCriteria(pageRequest.getSortCriteria()), pageRequest.getOffset(), pageRequest.getPageSize(), mongoCollection, JacsServiceData.class);
        return new PageResult<>(pageRequest, results);
    }

    @Override
    public PageResult<JacsServiceData> claimServiceByQueueAndState(String queueId, boolean onlyPreAssignedWork, Set<JacsServiceState> requestStates, PageRequest pageRequest) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(requestStates));
        Preconditions.checkArgument(StringUtils.isNotBlank(queueId));
        if (onlyPreAssignedWork) {
            return new PageResult<>(pageRequest, getOnlyPreAssignedServiceByQueueAndState(queueId, requestStates, pageRequest));
        } else {
            return new PageResult<>(pageRequest, claimNewOrPreAssignedServiceByQueueAndState(queueId, requestStates, pageRequest));
        }
    }

    private List<JacsServiceData> getOnlyPreAssignedServiceByQueueAndState(String queueId, Set<JacsServiceState> requestStates, PageRequest pageRequest) {
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        filtersBuilder.add(Filters.eq("queueId", queueId));
        filtersBuilder.add(in("state", requestStates));
        Bson bsonFilter = and(filtersBuilder.build());
        return find(bsonFilter, MongoDaoHelper.createBsonSortCriteria(pageRequest.getSortCriteria()), pageRequest.getOffset(), pageRequest.getPageSize(), JacsServiceData.class);
    }

    private List<JacsServiceData> claimNewOrPreAssignedServiceByQueueAndState(String queueId, Set<JacsServiceState> requestStates, PageRequest pageRequest) {
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        filtersBuilder.add(Filters.or(
                Filters.eq("queueId", queueId),
                Filters.exists("queueId", false)));
        filtersBuilder.add(in("state", requestStates));
        Bson bsonFilter = and(filtersBuilder.build());
        List<JacsServiceData> candidateResults = find(bsonFilter, MongoDaoHelper.createBsonSortCriteria(pageRequest.getSortCriteria()), pageRequest.getOffset(), pageRequest.getPageSize(), JacsServiceData.class);
        return candidateResults.stream()
                .map(sd -> {
                    FindOneAndUpdateOptions updateOptions = new FindOneAndUpdateOptions();
                    updateOptions.returnDocument(ReturnDocument.AFTER);

                    return mongoCollection.findOneAndUpdate(
                            Filters.and(
                                    Filters.eq("_id", sd.getId()),
                                    Filters.eq("accessId", sd.getAccessId()),
                                    Filters.or(
                                            Filters.eq("queueId", queueId),
                                            Filters.exists("queueId", false))
                            ),
                            Updates.combine(
                                    Updates.set("queueId", queueId),
                                    Updates.set("accessId", sd.nextAccessId())
                            ),
                            updateOptions
                    );
                })
                .filter(sd -> sd != null)
                .collect(Collectors.toList());
    }

    @Override
    public PageResult<JacsServiceData> findServicesByState(Set<JacsServiceState> requestStates, PageRequest pageRequest) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(requestStates));
        List<JacsServiceData> results = MongoDaoHelper.find(
                in("state", requestStates),
                MongoDaoHelper.createBsonSortCriteria(pageRequest.getSortCriteria()),
                pageRequest.getOffset(),
                pageRequest.getPageSize(),
                mongoCollection,
                JacsServiceData.class);
        return new PageResult<>(pageRequest, results);
    }

    @Override
    public void saveServiceHierarchy(JacsServiceData serviceData) {
        List<JacsServiceData> toBeInserted = new ArrayList<>();
        Map<JacsServiceData, Map<String, EntityFieldValueHandler<?>>> toBeUpdated = new LinkedHashMap<>();
        List<JacsServiceData> serviceHierarchy = serviceData.serviceHierarchyStream().map((JacsServiceData s) -> {
            if (s.getId() == null) {
                s.setId(idGenerator.generateId());
                s.initAccessId();
                toBeInserted.add(s);
                s.updateParentService(s.getParentService());
            } else {
                Map<String, EntityFieldValueHandler<?>> updates = s.updateParentService(s.getParentService());
                updates.put("state", new SetFieldValueHandler<>(s.getState()));
                if (toBeUpdated.get(s) == null) {
                    toBeUpdated.put(s, updates);
                } else {
                    toBeUpdated.get(s).putAll(updates);
                }
            }
            return s;
        }).collect(Collectors.toList());
        serviceHierarchy.stream().forEach(sd -> {
            sd.getDependencies().forEach(dependency -> sd.addServiceDependencyId(dependency));
            if (toBeUpdated.get(sd) != null) {
                toBeUpdated.get(sd).put("dependenciesIds", new SetFieldValueHandler<>(sd.getDependenciesIds()));
            }
        });
        if (CollectionUtils.isNotEmpty(toBeInserted)) mongoCollection.insertMany(toBeInserted);
        toBeUpdated.entrySet().forEach(updatedEntry -> update(updatedEntry.getKey(), updatedEntry.getValue()));
    }

    @Override
    public DaoUpdateResult update(JacsServiceData entity, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        Map<String, EntityFieldValueHandler<?>> serviceFieldsToUpdate = new LinkedHashMap<>(fieldsToUpdate);
        Date newModificationDate = new Date();
        int nextAccessId = entity.nextAccessId();
        serviceFieldsToUpdate.put("modificationDate", new SetFieldValueHandler<>(newModificationDate));
        serviceFieldsToUpdate.put("accessId", new SetFieldValueHandler<>(nextAccessId));
        DaoUpdateResult result = super.update(entity, serviceFieldsToUpdate);
        if (result.getEntitiesAffected() > 0) {
            entity.setModificationDate(newModificationDate);
            entity.setAccessId(nextAccessId);
        }
        return result;
    }

    protected Bson getUpdateMatchCriteria(JacsServiceData entity) {
        Bson idFilter = Filters.eq("_id", entity.getId());
        Bson accessIdFilter = Filters.eq("accessId", entity.getAccessId());
        return Filters.and(
                idFilter,
                accessIdFilter
        );
    }

}

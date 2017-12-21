package org.janelia.model.access.dao.mongo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.model.access.dao.JacsServiceDataDao;
import org.janelia.model.access.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.model.jacs2.AppendFieldValueHandler;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.service.JacsServiceEvent;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.jacs2.page.SortCriteria;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;

/**
 * Mongo based implementation of JacsServiceDataDao.
 */
public class JacsServiceDataMongoDao extends AbstractMongoDao<JacsServiceData> implements JacsServiceDataDao {

    @Inject
    public JacsServiceDataMongoDao(MongoDatabase mongoDatabase, @JacsDefault TimebasedIdentifierGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }

    @Override
    public List<JacsServiceData> findChildServices(Number serviceId) {
        return find(eq("parentServiceId", serviceId), null, 0, -1, JacsServiceData.class);
    }

    @Override
    public JacsServiceData findServiceHierarchy(Number serviceId) {
        JacsServiceData jacsServiceData = findById(serviceId);
        if (jacsServiceData == null) {
            return null;
        }
        Number rootServiceId = jacsServiceData.getRootServiceId();
        if (rootServiceId == null) {
            rootServiceId = serviceId;
        }
        Map<Number, JacsServiceData> fullServiceHierachy = new LinkedHashMap<>();
        find(Filters.or(eq("rootServiceId", rootServiceId), eq("_id", rootServiceId)), createBsonSortCriteria(ImmutableList.of(new SortCriteria("_id"))), 0, -1, JacsServiceData.class)
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

        return fullServiceHierachy.get(serviceId);
    }

    @Override
    public PageResult<JacsServiceData> findMatchingServices(JacsServiceData pattern, DataInterval<Date> creationInterval, PageRequest pageRequest) {
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        if (pattern.getId() != null) {
            filtersBuilder.add(eq("_id", pattern.getId()));
        }
        if (pattern.getParentServiceId() != null) {
            filtersBuilder.add(eq("parentServiceId", pattern.getParentServiceId()));
        }
        if (pattern.getRootServiceId() != null) {
            filtersBuilder.add(eq("rootServiceId", pattern.getRootServiceId()));
        }
        if (StringUtils.isNotBlank(pattern.getName())) {
            filtersBuilder.add(eq("name", pattern.getName()));
        }
        if (StringUtils.isNotBlank(pattern.getOwner())) {
            filtersBuilder.add(eq("owner", pattern.getOwner()));
        }
        if (StringUtils.isNotBlank(pattern.getVersion())) {
            filtersBuilder.add(eq("version", pattern.getVersion()));
        }
        if (pattern.getState() != null) {
            filtersBuilder.add(eq("state", pattern.getState()));
        }
        if (StringUtils.isNotBlank(pattern.getQueueId())) {
            filtersBuilder.add(eq("queueId", pattern.getQueueId()));
        }
        if (creationInterval.hasFrom()) {
            filtersBuilder.add(gte("creationDate", creationInterval.getFrom()));
        }
        if (creationInterval.hasTo()) {
            filtersBuilder.add(lt("creationDate", creationInterval.getTo()));
        }
        ImmutableList<Bson> filters = filtersBuilder.build();

        Bson bsonFilter = null;
        if (!filters.isEmpty()) bsonFilter = and(filters);
        List<JacsServiceData> results = find(bsonFilter, createBsonSortCriteria(pageRequest.getSortCriteria()), pageRequest.getOffset(), pageRequest.getPageSize(), JacsServiceData.class);
        return new PageResult<>(pageRequest, results);
    }

    @Override
    public PageResult<JacsServiceData> claimServiceByQueueAndState(String queueId, Set<JacsServiceState> requestStates, PageRequest pageRequest) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(requestStates));
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();

        if (StringUtils.isNotBlank(queueId)) {
            filtersBuilder.add(Filters.or(
                    Filters.eq("queueId", queueId),
                    Filters.exists("queueId", false)));
        } else {
            filtersBuilder.add(Filters.exists("queueId", false));
        }
        filtersBuilder.add(in("state", requestStates));
        Bson bsonFilter = and(filtersBuilder.build());
        List<JacsServiceData> candidateResults = find(bsonFilter, createBsonSortCriteria(pageRequest.getSortCriteria()), pageRequest.getOffset(), pageRequest.getPageSize(), JacsServiceData.class);
        if (candidateResults.isEmpty()) {
            return new PageResult<>(pageRequest, candidateResults);
        }
        List<JacsServiceData> finalClaimedResults = candidateResults.stream()
                .map(sd -> {
                    FindOneAndUpdateOptions updateOptions = new FindOneAndUpdateOptions();
                    updateOptions.returnDocument(ReturnDocument.AFTER);

                    return mongoCollection.findOneAndUpdate(
                            Filters.and(
                                    Filters.eq("_id", sd.getId()),
                                    Filters.or(
                                            Filters.eq("queueId", queueId),
                                            Filters.exists("queueId", false))
                            ),
                            Updates.set("queueId", queueId),
                            updateOptions
                    );
                })
                .filter(sd -> sd != null)
                .collect(Collectors.toList());
        return new PageResult<>(pageRequest, finalClaimedResults);
    }

    @Override
    public PageResult<JacsServiceData> findServicesByState(Set<JacsServiceState> requestStates, PageRequest pageRequest) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(requestStates));
        List<JacsServiceData> results = find(in("state", requestStates), createBsonSortCriteria(pageRequest.getSortCriteria()), pageRequest.getOffset(), pageRequest.getPageSize(), JacsServiceData.class);
        return new PageResult<>(pageRequest, results);
    }

    @Override
    public void saveServiceHierarchy(JacsServiceData serviceData) {
        List<JacsServiceData> toBeInserted = new ArrayList<>();
        Map<JacsServiceData, Map<String, EntityFieldValueHandler<?>>> toBeUpdated = new LinkedHashMap<>();
        List<JacsServiceData> serviceHierarchy = serviceData.serviceHierarchyStream().map(s -> {
            if (s.getId() == null) {
                s.setId(idGenerator.generateId());
                toBeInserted.add(s);
                s.updateParentService(s.getParentService());
            } else {
                Map<String, EntityFieldValueHandler<?>> updates = s.updateParentService(s.getParentService());
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
        mongoCollection.insertMany(toBeInserted);
        toBeUpdated.entrySet().forEach(updatedEntry -> update(updatedEntry.getKey(), updatedEntry.getValue()));
    }

    @Override
    public void update(JacsServiceData entity, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        Map<String, EntityFieldValueHandler<?>> fieldsWithUpdatedDate = new LinkedHashMap<>(fieldsToUpdate);
        entity.setModificationDate(new Date());
        fieldsWithUpdatedDate.put("modificationDate", new SetFieldValueHandler<>(entity.getModificationDate()));
        super.update(entity, fieldsWithUpdatedDate);
    }

    @Override
    public void addServiceEvent(JacsServiceData jacsServiceData, JacsServiceEvent serviceEvent) {
        Map<String, EntityFieldValueHandler<?>> updatedFields = new HashMap<>();
        updatedFields.put("events", new AppendFieldValueHandler<>(serviceEvent));
        update(jacsServiceData, updatedFields);
    }

    @Override
    public void updateServiceResult(JacsServiceData serviceData) {
        Map<String, EntityFieldValueHandler<?>> updatedFields = new HashMap<>();
        updatedFields.put("serializableResult", new SetFieldValueHandler<>(serviceData.getSerializableResult()));
        update(serviceData, updatedFields);
    }
}

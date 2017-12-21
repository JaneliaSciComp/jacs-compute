package org.janelia.model.access.dao.mongo;

import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.janelia.model.access.dao.JacsServiceDataArchiveDao;
import org.janelia.model.jacs2.page.SortCriteria;
import org.janelia.model.service.JacsServiceData;

import javax.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.mongodb.client.model.Filters.eq;

/**
 * Archived JacsServiceData Mongo based implementation.
 */
public class JacsServiceDataArchiveMongoDao extends ArchiveMongoDao<JacsServiceData> implements JacsServiceDataArchiveDao {

    @Inject
    public JacsServiceDataArchiveMongoDao(MongoDatabase mongoDatabase) {
        super(mongoDatabase);
    }

    @Override
    public JacsServiceData findArchivedServiceHierarchy(Number serviceId) {
        JacsServiceData jacsServiceData = findArchivedEntityById(serviceId);
        if (jacsServiceData == null) {
            return null;
        }
        Number rootServiceId = jacsServiceData.getRootServiceId();
        if (rootServiceId == null) {
            rootServiceId = serviceId;
        }
        Map<Number, JacsServiceData> fullServiceHierachy = new LinkedHashMap<>();
        MongoDaoHelper.find(Filters.or(eq("rootServiceId", rootServiceId), eq("_id", rootServiceId)),
                MongoDaoHelper.createBsonSortCriteria(ImmutableList.of(new SortCriteria("_id"))),
                0, -1,
                archiveMongoCollection,
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
        return fullServiceHierachy.get(serviceId);
    }
}

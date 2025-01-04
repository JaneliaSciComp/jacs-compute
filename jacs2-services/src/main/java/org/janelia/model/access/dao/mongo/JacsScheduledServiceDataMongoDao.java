package org.janelia.model.access.dao.mongo;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexModel;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;

import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.janelia.jacs2.cdi.qualifier.BoolPropertyValue;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.model.access.dao.JacsScheduledServiceDataDao;
import org.janelia.model.access.domain.IdGenerator;
import org.janelia.model.service.JacsScheduledServiceData;

/**
 * Mongo based implementation of JacsServiceDataDao.
 */
@ApplicationScoped
public class JacsScheduledServiceDataMongoDao extends AbstractMongoDao<JacsScheduledServiceData> implements JacsScheduledServiceDataDao {

    @Inject
    public JacsScheduledServiceDataMongoDao(MongoDatabase mongoDatabase,
                                            @JacsDefault IdGenerator<Long> idGenerator,
                                            @BoolPropertyValue(name = "MongoDB.createCollectionIndexes") boolean createCollectionIndexes) {
        super(mongoDatabase, idGenerator);
        if (createCollectionIndexes) {
            mongoCollection.createIndexes(
                    ImmutableList.of(
                            new IndexModel(Indexes.ascending("nextStartTime"))
                    )
            );
        }
    }

    @Override
    public List<JacsScheduledServiceData> findServicesScheduledAtOrBefore(String queueId, Date scheduledTime, boolean includeDisabled) {
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        if (StringUtils.isNotBlank(queueId)) {
            // if a queue is provided lookup only services for the specified queue
            filtersBuilder.add(Filters.or(
                    Filters.eq("serviceQueueId", queueId),
                    Filters.exists("serviceQueueId", false)));
        } else {
            // otherwise only lookup scheduled services that are not explicitly assigned to a queue
            filtersBuilder.add(Filters.exists("serviceQueueId", false));
        }

        filtersBuilder.add(Filters.or(
                Filters.lte("nextStartTime", scheduledTime),
                Filters.exists("nextStartTime", false)));

        if (!includeDisabled) {
            // if it does not explicitly request disabled services only get the ones that are not disabled
            filtersBuilder.add(Filters.or(
                    Filters.eq("disabled", false),
                    Filters.exists("disabled", false)));
        } // otherwise don't use disabled flag for filtering

        Bson queryFilter = Filters.and(filtersBuilder.build());

        return find(queryFilter, null, 0, -1, getEntityType());
    }

    @Override
    public Optional<JacsScheduledServiceData> updateServiceScheduledTime(JacsScheduledServiceData scheduledServiceData, Date currentScheduledTime) {
        FindOneAndUpdateOptions updateOptions = new FindOneAndUpdateOptions();
        updateOptions.returnDocument(ReturnDocument.AFTER);

        JacsScheduledServiceData updatedScheduledServiceData = mongoCollection.findOneAndUpdate(
                Filters.and(
                        Filters.eq("_id", scheduledServiceData.getId()),
                        Filters.or(
                                Filters.lte("nextStartTime", currentScheduledTime),
                                Filters.exists("nextStartTime", false)),
                        Filters.or(
                                Filters.eq("disabled", false),
                                Filters.exists("disabled", false))
                ),
                Updates.combine(
                        Updates.set("nextStartTime", scheduledServiceData.getNextStartTime()),
                        Updates.set("lastStartTime", currentScheduledTime)
                ),

                updateOptions
        );
        return updatedScheduledServiceData == null ? Optional.empty() : Optional.of(updatedScheduledServiceData);
    }
}

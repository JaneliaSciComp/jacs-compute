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
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.model.access.dao.JacsScheduledServiceDataDao;
import org.janelia.model.access.dao.JacsServiceDataDao;
import org.janelia.model.access.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.jacs2.page.SortCriteria;
import org.janelia.model.service.JacsScheduledServiceData;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;

import javax.inject.Inject;
import javax.swing.text.html.Option;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;

/**
 * Mongo based implementation of JacsServiceDataDao.
 */
public class JacsScheduledServiceDataMongoDao extends AbstractMongoDao<JacsScheduledServiceData> implements JacsScheduledServiceDataDao {

    @Inject
    public JacsScheduledServiceDataMongoDao(MongoDatabase mongoDatabase, @JacsDefault TimebasedIdentifierGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
        mongoCollection.createIndexes(
                ImmutableList.of(
                        new IndexModel(Indexes.ascending("nextStartTime"))
                )
        );
    }

    @Override
    public List<JacsScheduledServiceData> findServiceScheduledAfter(String queueId, Date scheduledTime, boolean includeDisabled) {
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
                Filters.gt("nextStartTime", scheduledTime),
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
                                Filters.gt("nextStartTime", currentScheduledTime),
                                Filters.exists("nextStartTime", false)),
                        Filters.or(
                                Filters.eq("disabled", false),
                                Filters.exists("disabled", false))
                ),
                Updates.set("nextStartTime", scheduledServiceData.getNextStartTime()),
                updateOptions
        );
        return updatedScheduledServiceData == null ? Optional.empty() : Optional.of(updatedScheduledServiceData);
    }
}

package org.janelia.model.access.dao.mongo;

import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.service.JacsServiceData;

import java.util.Date;

import com.mongodb.client.model.Filters;

class JacsServiceDataMongoHelper {

    static Bson createBsonMatchingFilter(JacsServiceData pattern, DataInterval<Date> creationInterval) {
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        if (pattern.getId() != null) {
            filtersBuilder.add(Filters.eq("_id", pattern.getId()));
        }
        if (pattern.getParentServiceId() != null) {
            filtersBuilder.add(Filters.eq("parentServiceId", pattern.getParentServiceId()));
        }
        if (pattern.getRootServiceId() != null) {
            filtersBuilder.add(Filters.eq("rootServiceId", pattern.getRootServiceId()));
        }
        if (StringUtils.isNotBlank(pattern.getName())) {
            filtersBuilder.add(Filters.eq("name", pattern.getName()));
        }
        if (StringUtils.isNotBlank(pattern.getOwnerKey())) {
            filtersBuilder.add(Filters.eq("ownerKey", pattern.getOwnerKey()));
        }
        if (StringUtils.isNotBlank(pattern.getVersion())) {
            filtersBuilder.add(Filters.eq("version", pattern.getVersion()));
        }
        if (pattern.getState() != null) {
            filtersBuilder.add(Filters.eq("state", pattern.getState()));
        }
        if (StringUtils.isNotBlank(pattern.getQueueId())) {
            filtersBuilder.add(Filters.eq("queueId", pattern.getQueueId()));
        }
        if (CollectionUtils.isNotEmpty(pattern.getTags())) {
            filtersBuilder.add(Filters.all("tags", pattern.getTags()));
        }
        if (creationInterval.hasFrom()) {
            filtersBuilder.add(Filters.gte("creationDate", creationInterval.getFrom()));
        }
        if (creationInterval.hasTo()) {
            filtersBuilder.add(Filters.lt("creationDate", creationInterval.getTo()));
        }
        ImmutableList<Bson> filters = filtersBuilder.build();
        if (!filters.isEmpty()) {
            return Filters.and(filters);
        } else {
            return null;
        }
    }

}

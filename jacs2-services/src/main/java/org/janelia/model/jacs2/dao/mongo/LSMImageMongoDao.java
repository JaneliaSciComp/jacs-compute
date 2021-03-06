package org.janelia.model.jacs2.dao.mongo;

import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.janelia.jacs2.cdi.qualifier.Jacs2Future;
import org.janelia.model.access.dao.mongo.MongoDaoHelper;
import org.janelia.model.access.domain.IdGenerator;
import org.janelia.model.jacs2.domain.Subject;
import org.janelia.model.jacs2.domain.sample.LSMImage;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.model.jacs2.dao.LSMImageDao;
import org.janelia.model.jacs2.DomainModelUtils;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;

import javax.inject.Inject;

import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class LSMImageMongoDao extends AbstractImageMongoDao<LSMImage> implements LSMImageDao {
    @Inject
    @Jacs2Future
    public LSMImageMongoDao(MongoDatabase mongoDatabase, @JacsDefault IdGenerator<Long> idGenerator) {
        super(mongoDatabase, idGenerator);
    }

    @Override
    public PageResult<LSMImage> findMatchingLSMs(Subject subject, LSMImage pattern, PageRequest pageRequest) {
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        if (pattern.getId() != null) {
            filtersBuilder.add(eq("_id", pattern.getId()));
        }
        if (StringUtils.isNotBlank(pattern.getName())) {
            filtersBuilder.add(eq("name", pattern.getName()));
        }
        if (StringUtils.isNotBlank(pattern.getOwnerKey())) {
            // lookup samples that have a matching owner or don't have it set at all.
            filtersBuilder.add(
                    Filters.or(
                            eq("ownerKey", pattern.getOwnerKey()),
                            Filters.exists("ownerKey", false)
                    )
            );
        }
        if (StringUtils.isNotBlank(pattern.getAge())) {
            filtersBuilder.add(eq("age", pattern.getAge()));
        }
        if (StringUtils.isNotBlank(pattern.getDataSet())) {
            filtersBuilder.add(eq("dataSet", pattern.getDataSet()));
        }
        if (StringUtils.isNotBlank(pattern.getLine())) {
            filtersBuilder.add(eq("line", pattern.getLine()));
        }
        if (StringUtils.isNotBlank(pattern.getSlideCode())) {
            filtersBuilder.add(eq("slideCode", pattern.getSlideCode()));
        }
        if (pattern.getSageId() != null && pattern.getSageId() != 0) {
            filtersBuilder.add(eq("sageId", pattern.getSageId()));
        }
        if (DomainModelUtils.isNotAdmin(subject)) {
            filtersBuilder.add(createSubjectReadPermissionFilter(subject));
        }

        ImmutableList<Bson> filters = filtersBuilder.build();

        Bson bsonFilter = null;
        if (!filters.isEmpty()) bsonFilter = and(filters);
        List<LSMImage> results = find(
                bsonFilter,
                MongoDaoHelper.createBsonSortCriteria(pageRequest.getSortCriteria()),
                pageRequest.getOffset(),
                pageRequest.getPageSize(),
                LSMImage.class);
        return new PageResult<>(pageRequest, results);
    }
}

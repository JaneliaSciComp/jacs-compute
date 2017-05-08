package org.janelia.jacs2.dao.mongo;

import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.janelia.it.jacs.model.domain.Subject;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.dao.LSMImageDao;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.model.DomainModelUtils;
import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.page.PageResult;

import javax.inject.Inject;

import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

public class LSMImageMongoDao extends AbstractImageMongoDao<LSMImage> implements LSMImageDao {
    @Inject
    public LSMImageMongoDao(MongoDatabase mongoDatabase, TimebasedIdentifierGenerator idGenerator, ObjectMapperFactory objectMapperFactory) {
        super(mongoDatabase, idGenerator, objectMapperFactory);
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
            filtersBuilder.add(eq("ownerKey", pattern.getOwnerKey()));
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
        List<LSMImage> results = find(bsonFilter, createBsonSortCriteria(pageRequest.getSortCriteria()), pageRequest.getOffset(), pageRequest.getPageSize(), LSMImage.class);
        return new PageResult<>(pageRequest, results);
    }
}

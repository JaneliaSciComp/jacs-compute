package org.janelia.jacs2.dao.mongo;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.Subject;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.dao.DatasetDao;
import org.janelia.it.jacs.model.domain.sample.DataSet;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.model.DomainModelUtils;

import javax.inject.Inject;
import java.util.List;

public class DatasetMongoDao extends AbstractDomainObjectDao<DataSet> implements DatasetDao {
    @Inject
    public DatasetMongoDao(MongoDatabase mongoDatabase, TimebasedIdentifierGenerator idGenerator, ObjectMapperFactory objectMapperFactory) {
        super(mongoDatabase, idGenerator, objectMapperFactory);
    }

    @Override
    public DataSet findByName(Subject subject, String datasetName) {
        if (StringUtils.isBlank(datasetName)) {
            return null;
        }
        List<DataSet> results;
        if (DomainModelUtils.isAdminOrUndefined(subject)) {
            results = find(Filters.eq("name", datasetName),
                    null,
                    0,
                    -1,
                    DataSet.class);
        } else {
            results = find(
                    Filters.and(
                            createSubjectReadPermissionFilter(subject),
                            Filters.eq("name", datasetName)
                    ),
                    null,
                    0,
                    -1,
                    DataSet.class);
        }
        if (CollectionUtils.isEmpty(results)) {
            return null;
        } else if (results.size() == 1) {
            return results.get(0);
        } else {
            throw new IllegalStateException("More than one record found with the name: " + datasetName + " (" + results.size() + ")");
        }
    }
}

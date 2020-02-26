package org.janelia.model.jacs2.dao.mongo;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.Jacs2Future;
import org.janelia.model.jacs2.domain.Subject;
import org.janelia.model.jacs2.domain.sample.DataSet;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.model.jacs2.dao.DatasetDao;
import org.janelia.model.util.TimebasedIdentifierGenerator;
import org.janelia.model.jacs2.DomainModelUtils;

import javax.inject.Inject;
import java.util.List;

public class DatasetMongoDao extends AbstractDomainObjectDao<DataSet> implements DatasetDao {
    @Inject
    @Jacs2Future
    public DatasetMongoDao(MongoDatabase mongoDatabase, @JacsDefault TimebasedIdentifierGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }

    @Override
    public DataSet findByNameOrIdentifier(Subject subject, String datasetNameOrIdentifier) {
        if (StringUtils.isBlank(datasetNameOrIdentifier)) {
            return null;
        }
        List<DataSet> results;
        if (DomainModelUtils.isAdminOrUndefined(subject)) {
            results = find(
                    Filters.or(Filters.eq("identifier", datasetNameOrIdentifier), Filters.eq("name", datasetNameOrIdentifier)),
                    null,
                    0,
                    -1,
                    DataSet.class);
        } else {
            results = find(
                    Filters.and(
                            createSubjectReadPermissionFilter(subject),
                            Filters.or(Filters.eq("identifier", datasetNameOrIdentifier), Filters.eq("name", datasetNameOrIdentifier))
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
            throw new IllegalStateException("More than one record found with the name: " + datasetNameOrIdentifier + " (" + results.size() + ")");
        }
    }
}

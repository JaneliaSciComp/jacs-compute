package org.janelia.model.jacs2.dao;

import org.janelia.model.jacs2.domain.Subject;
import org.janelia.model.jacs2.domain.sample.DataSet;

public interface DatasetDao extends DomainObjectDao<DataSet> {
    DataSet findByNameOrIdentifier(Subject subject, String datasetName);
}

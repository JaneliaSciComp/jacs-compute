package org.janelia.jacs2.asyncservice.sample;

import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor2;
import org.janelia.model.access.domain.DomainDAO;
import org.janelia.model.access.domain.IndexCreation;
import org.janelia.model.service.JacsServiceData;

import javax.inject.Inject;
import javax.inject.Named;

@Named("dbIndexing")

@Service(description="Ensures that all necessary indexes are created on the database")

public class DatabaseIndexingService extends AbstractBasicLifeCycleServiceProcessor2<Void> {

    @Inject
    private DomainDAO domainDao;

    @Override
    protected void execute(JacsServiceData sd) throws Exception {
        IndexCreation indexCreation = new IndexCreation(domainDao);
        indexCreation.ensureIndexes();
        logger.info("Indexing complete.");
    }
}

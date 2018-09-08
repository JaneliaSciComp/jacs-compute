package org.janelia.jacs2.cdi;

import com.mongodb.MongoClient;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.domain.DomainDAO;
import org.janelia.model.access.domain.LockingDAO;
import org.janelia.model.access.domain.WorkflowDAO;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

@ApplicationScoped
public class DaoProducer {

    @Produces
    public DomainDAO createDomainDAO(MongoClient mongoClient,
                                     @PropertyValue(name = "MongoDB.Database") String databaseName) {
        return new DomainDAO(mongoClient, databaseName);
    }

    @Produces
    public WorkflowDAO createWorkflowDAO(MongoClient mongoClient,
                                         @PropertyValue(name = "MongoDB.Database") String databaseName) {
        return new WorkflowDAO(mongoClient, databaseName);
    }

    @Produces
    public LockingDAO createLockingDAO(MongoClient mongoClient,
                                       @PropertyValue(name = "MongoDB.Database") String databaseName) {
        return new LockingDAO(mongoClient, databaseName);
    }
}

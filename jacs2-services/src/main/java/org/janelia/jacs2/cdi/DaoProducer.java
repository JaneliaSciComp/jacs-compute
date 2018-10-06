package org.janelia.jacs2.cdi;

import com.mongodb.MongoClient;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.domain.DomainDAO;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

@ApplicationScoped
public class DaoProducer {

    @Produces
    public DomainDAO createDomainDAO(
            MongoClient mongoClient,
            @PropertyValue(name = "MongoDB.Database") String databaseName) {
        return new DomainDAO(mongoClient, databaseName);
    }

}

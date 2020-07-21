package org.janelia.jacs2.cdi;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import com.mongodb.MongoClient;

import org.janelia.jacs2.cdi.qualifier.JacsLegacy;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.domain.DomainDAO;

@ApplicationScoped
public class DaoProducer {

    @Produces
    public DomainDAO createDomainDAO(
            @JacsLegacy MongoClient mongoClient,
            @PropertyValue(name = "MongoDB.Database") String databaseName) {
        return new DomainDAO(mongoClient, databaseName);
    }

}

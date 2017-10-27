package org.janelia.model.jacs2.dao.mongo;

import com.mongodb.client.MongoDatabase;
import org.janelia.jacs2.cdi.qualifier.Jacs2Future;
import org.janelia.model.jacs2.domain.sample.NeuronFragment;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.model.jacs2.dao.NeuronFragmentDao;
import org.janelia.model.access.dao.mongo.utils.TimebasedIdentifierGenerator;

import javax.inject.Inject;

public class NeuronFragmentMongoDao extends AbstractDomainObjectDao<NeuronFragment> implements NeuronFragmentDao {
    @Inject
    @Jacs2Future
    public NeuronFragmentMongoDao(MongoDatabase mongoDatabase, @JacsDefault TimebasedIdentifierGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }
}

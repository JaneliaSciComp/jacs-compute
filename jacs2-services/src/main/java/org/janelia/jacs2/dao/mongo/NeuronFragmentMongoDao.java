package org.janelia.jacs2.dao.mongo;

import com.mongodb.client.MongoDatabase;
import org.janelia.it.jacs.model.domain.sample.NeuronFragment;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.jacs2.dao.NeuronFragmentDao;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;

import javax.inject.Inject;

public class NeuronFragmentMongoDao extends AbstractDomainObjectDao<NeuronFragment> implements NeuronFragmentDao {
    @Inject
    public NeuronFragmentMongoDao(MongoDatabase mongoDatabase, @JacsDefault TimebasedIdentifierGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }
}

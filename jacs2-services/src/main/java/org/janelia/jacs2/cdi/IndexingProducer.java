package org.janelia.jacs2.cdi;

import org.janelia.messaging.core.MessageSender;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.model.access.search.AsyncDomainObjectIndexer;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

@ApplicationScoped
public class IndexingProducer {

    @ApplicationScoped
    @AsyncIndex
    @Produces
    public DomainObjectIndexer createDomainObjectIndexer(MessageSender mesageSender) {
        return new AsyncDomainObjectIndexer(mesageSender);
    }

}

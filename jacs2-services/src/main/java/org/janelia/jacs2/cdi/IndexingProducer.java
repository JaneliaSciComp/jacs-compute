package org.janelia.jacs2.cdi;

import java.util.concurrent.ExecutorService;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.apache.solr.client.solrj.SolrServer;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.dataservice.search.DomainObjectIndexerProvider;
import org.janelia.messaging.core.MessageSender;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.model.access.domain.search.SolrBasedDomainObjectIndexer;
import org.janelia.model.access.search.AsyncDomainObjectIndexer;
import org.janelia.model.domain.DomainObjectGetter;
import org.janelia.model.domain.ontology.DomainAnnotationGetter;
import org.janelia.model.domain.workspace.NodeAncestorsGetter;

@ApplicationScoped
public class IndexingProducer {

    @AsyncIndex
    @ApplicationScoped
    @Produces
    public DomainObjectIndexer createDomainObjectIndexer(@AsyncIndex MessageSender mesageSender, @AsyncIndex ExecutorService indexingExecutor) {
        return new AsyncDomainObjectIndexer(mesageSender, indexingExecutor);
    }

    @Produces
    public DomainObjectIndexerProvider<SolrServer> createSolrBasedDomainObjectIndexerProvider(NodeAncestorsGetter nodeAncestorsGetter,
                                                                                              DomainAnnotationGetter nodeAnnotationGetter,
                                                                                              DomainObjectGetter objectGetter,
                                                                                              @IntPropertyValue(name = "Solr.BatchSize", defaultValue = 20000) int solrBatchSize,
                                                                                              @IntPropertyValue(name = "Solr.CommitSize", defaultValue = 200000) int solrCommitSize) {
        return (SolrServer solrServer) -> new SolrBasedDomainObjectIndexer(solrServer,
                nodeAncestorsGetter,
                nodeAnnotationGetter,
                objectGetter,
                solrBatchSize,
                solrCommitSize)
                ;
    }
}

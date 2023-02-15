package org.janelia.jacs2.cdi;

import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;

import org.apache.solr.client.solrj.SolrClient;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.dataservice.search.DocumentIndexingService;
import org.janelia.jacs2.dataservice.search.DomainObjectIndexerProvider;
import org.janelia.jacs2.dataservice.search.SolrConfig;
import org.janelia.messaging.core.MessageSender;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.cdi.WithCache;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.NodeDao;
import org.janelia.model.access.domain.nodetools.AllNodeAncestorsGetterImpl;
import org.janelia.model.access.domain.nodetools.CachedAllNodeAncestorsGetterImpl;
import org.janelia.model.access.domain.nodetools.DirectNodeAncestorsGetterImpl;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.model.access.domain.search.SolrBasedDomainObjectIndexer;
import org.janelia.model.access.search.AsyncDomainObjectIndexer;
import org.janelia.model.domain.DomainObjectGetter;
import org.janelia.model.domain.ontology.DomainAnnotationGetter;
import org.janelia.model.domain.workspace.Node;

@ApplicationScoped
public class IndexingProducer {

    @AsyncIndex
    @ApplicationScoped
    @Produces
    public DomainObjectIndexer createDomainObjectIndexer(@AsyncIndex MessageSender mesageSender, @AsyncIndex ExecutorService indexingExecutor) {
        return new AsyncDomainObjectIndexer(mesageSender, indexingExecutor);
    }

    @Produces
    public DomainObjectIndexerProvider<SolrClient> createIndexerProviderForRealTimeIndexing(
            Instance<NodeDao<? extends Node>> nodeDaosProvider,
            DomainAnnotationGetter nodeAnnotationGetter,
            DomainObjectGetter objectGetter,
            @IntPropertyValue(name = "Solr.BatchSize", defaultValue = 20000) int solrBatchSize) {
        return (SolrClient solrClient) -> new SolrBasedDomainObjectIndexer(solrClient,
                nodeDaosProvider.stream()
                        .map(nodeDao -> new AllNodeAncestorsGetterImpl<>(new DirectNodeAncestorsGetterImpl<>(nodeDao)))
                        .collect(Collectors.toList()),
                nodeAnnotationGetter,
                objectGetter,
                solrBatchSize)
                ;
    }

    /**
     * For rebuilding the index we use an object indexer that uses temporary cached ancestors and annotation in order
     * to minimize the trips to the database.
     *
     * @param nodeDaosProvider
     * @param nodeAnnotationGetterProvider
     * @param objectGetter
     * @param solrBatchSize
     * @return an indexer provider for DomainObject
     */
    @WithCache
    @Produces
    public DomainObjectIndexerProvider<SolrClient> createIndexerProviderForIndexRebuild(
            Instance<NodeDao<? extends Node>> nodeDaosProvider,
            @WithCache Instance<DomainAnnotationGetter> nodeAnnotationGetterProvider,
            DomainObjectGetter objectGetter,
            @IntPropertyValue(name = "Solr.BatchSize", defaultValue = 20000) int solrBatchSize) {
        return (SolrClient solrClient) -> new SolrBasedDomainObjectIndexer(solrClient,
                nodeDaosProvider.stream()
                        .map(CachedAllNodeAncestorsGetterImpl::new)
                        .collect(Collectors.toList()),
                nodeAnnotationGetterProvider.get(),
                objectGetter,
                solrBatchSize)
                ;
    }

    @Produces
    public DocumentIndexingService createDocumentIndexService(LegacyDomainDao legacyDomainDao,
                                                              SolrConfig solrConfig,
                                                              DomainObjectIndexerProvider<SolrClient> domainObjectIndexerProvider) {
        return new DocumentIndexingService(legacyDomainDao, solrConfig, domainObjectIndexerProvider);
    }

    @WithCache
    @Produces
    public DocumentIndexingService createDocumentIndexServiceWithCachedData(LegacyDomainDao legacyDomainDao,
                                                                            SolrConfig solrConfig,
                                                                            @WithCache DomainObjectIndexerProvider<SolrClient> domainObjectIndexerProvider) {
        return new DocumentIndexingService(legacyDomainDao, solrConfig, domainObjectIndexerProvider);
    }

}

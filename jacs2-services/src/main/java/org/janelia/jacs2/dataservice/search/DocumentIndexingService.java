package org.janelia.jacs2.dataservice.search;

import java.util.List;
import java.util.Set;

import javax.enterprise.inject.Vetoed;
import javax.inject.Inject;

import org.apache.solr.client.solrj.SolrServer;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.search.DocumentSearchParams;
import org.janelia.model.access.domain.search.DocumentSearchResults;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.sample.NeuronFragment;

/**
 * A SOLR indexer.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class DocumentIndexingService extends AbstractIndexingServiceSupport {

    public DocumentIndexingService(LegacyDomainDao legacyDomainDao,
                                   SolrConfig solrConfig,
                                   DomainObjectIndexerProvider<SolrServer> domainObjectIndexerProvider) {
        super(legacyDomainDao, solrConfig, domainObjectIndexerProvider);
    }

    public int indexDocuments(List<Reference> domainObjectReferences) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerProvider.createDomainObjectIndexer(
                createSolrBuilder().setSolrCore(solrConfig.getSolrMainCore()).build());
        return domainObjectIndexer.indexDocumentStream(
                legacyDomainDao.iterateDomainObjects(domainObjectReferences)
                        .filter(d -> d instanceof NeuronFragment) // skip NeuronFragment references from indexing
        );
    }

    public int removeDocuments(List<Long> ids) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerProvider.createDomainObjectIndexer(
                createSolrBuilder().setSolrCore(solrConfig.getSolrMainCore()).build());
        return domainObjectIndexer.removeDocumentStream(ids.stream());
    }

    public DocumentSearchResults searchIndex(DocumentSearchParams searchParams) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerProvider.createDomainObjectIndexer(
                createSolrBuilder().setSolrCore(solrConfig.getSolrMainCore()).build());
        return domainObjectIndexer.searchIndex(searchParams);
    }

    public void updateDocsAncestors(Set<Long> docIds, Long ancestorId) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerProvider.createDomainObjectIndexer(
                createSolrBuilder().setSolrCore(solrConfig.getSolrMainCore()).build());
        domainObjectIndexer.updateDocsAncestors(docIds, ancestorId);
    }

}

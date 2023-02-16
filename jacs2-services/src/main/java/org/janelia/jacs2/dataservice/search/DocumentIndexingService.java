package org.janelia.jacs2.dataservice.search;

import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrClient;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.ReferenceDomainObjectReadDao;
import org.janelia.model.access.domain.search.DocumentSearchParams;
import org.janelia.model.access.domain.search.DocumentSearchResults;
import org.janelia.model.access.domain.search.DomainObjectIndexer;
import org.janelia.model.domain.Reference;

/**
 * A SOLR indexer.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class DocumentIndexingService extends AbstractIndexingServiceSupport {

    public DocumentIndexingService(ReferenceDomainObjectReadDao referenceDomainObjectReadDao,
                                   SolrConfig solrConfig,
                                   DomainObjectIndexerProvider<SolrClient> domainObjectIndexerProvider) {
        super(referenceDomainObjectReadDao, solrConfig, domainObjectIndexerProvider);
    }

    public int indexDocuments(List<Reference> domainObjectReferences) {
        DomainObjectIndexer domainObjectIndexer = domainObjectIndexerProvider.createDomainObjectIndexer(
                createSolrBuilder().setSolrCore(solrConfig.getSolrMainCore()).build());
        return domainObjectIndexer.indexDocumentStream(referenceDomainObjectReadDao.findByReferences(domainObjectReferences).stream());
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

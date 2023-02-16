package org.janelia.jacs2.dataservice.search;

import org.apache.solr.client.solrj.SolrClient;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.ReferenceDomainObjectReadDao;

/**
 * A SOLR indexer.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
abstract class AbstractIndexingServiceSupport {

    final ReferenceDomainObjectReadDao referenceDomainObjectReadDao;
    final DomainObjectIndexerProvider<SolrClient> domainObjectIndexerProvider;
    final SolrConfig solrConfig;

    AbstractIndexingServiceSupport(ReferenceDomainObjectReadDao referenceDomainObjectReadDao,
                                   SolrConfig solrConfig,
                                   DomainObjectIndexerProvider<SolrClient> domainObjectIndexerProvider) {
        this.referenceDomainObjectReadDao = referenceDomainObjectReadDao;
        this.solrConfig = solrConfig;
        this.domainObjectIndexerProvider = domainObjectIndexerProvider;
    }

    SolrBuilder createSolrBuilder() {
        return solrConfig.builder();
    }

}

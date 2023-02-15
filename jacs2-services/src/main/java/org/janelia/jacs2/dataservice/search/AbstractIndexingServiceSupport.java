package org.janelia.jacs2.dataservice.search;

import org.apache.solr.client.solrj.SolrClient;
import org.janelia.model.access.dao.LegacyDomainDao;

/**
 * A SOLR indexer.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
abstract class AbstractIndexingServiceSupport {

    final LegacyDomainDao legacyDomainDao;
    final DomainObjectIndexerProvider<SolrClient> domainObjectIndexerProvider;
    final SolrConfig solrConfig;

    AbstractIndexingServiceSupport(LegacyDomainDao legacyDomainDao,
                                   SolrConfig solrConfig,
                                   DomainObjectIndexerProvider<SolrClient> domainObjectIndexerProvider) {
        this.legacyDomainDao = legacyDomainDao;
        this.solrConfig = solrConfig;
        this.domainObjectIndexerProvider = domainObjectIndexerProvider;
    }

    SolrBuilder createSolrBuilder() {
        return solrConfig.builder();
    }

}

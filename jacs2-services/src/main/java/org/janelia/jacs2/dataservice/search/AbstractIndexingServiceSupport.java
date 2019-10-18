package org.janelia.jacs2.dataservice.search;

import javax.inject.Inject;

import org.apache.solr.client.solrj.SolrServer;
import org.janelia.model.access.dao.LegacyDomainDao;

/**
 * A SOLR indexer.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
abstract class AbstractIndexingServiceSupport {

    final LegacyDomainDao legacyDomainDao;
    final DomainObjectIndexerProvider<SolrServer> domainObjectIndexerProvider;
    final SolrConfig solrConfig;

    @Inject
    AbstractIndexingServiceSupport(LegacyDomainDao legacyDomainDao,
                                   SolrConfig solrConfig,
                                   DomainObjectIndexerProvider<SolrServer> domainObjectIndexerProvider) {
        this.legacyDomainDao = legacyDomainDao;
        this.solrConfig = solrConfig;
        this.domainObjectIndexerProvider = domainObjectIndexerProvider;
    }

    SolrBuilder createSolrBuilder() {
        return solrConfig.builder();
    }
}

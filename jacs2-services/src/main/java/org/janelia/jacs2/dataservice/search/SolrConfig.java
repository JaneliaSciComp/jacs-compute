package org.janelia.jacs2.dataservice.search;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacs2.cdi.qualifier.OnStartup;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SOLR configuration.
 */
@OnStartup
@ApplicationScoped
public class SolrConfig {

    private static final Logger LOG = LoggerFactory.getLogger(SolrConfig.class);

    private final String solrServerBaseURL;
    private final String solrMainCore;
    private final String solrBuildCore;
    private final int solrLoaderQueueSize;
    private final int solrLoaderThreadCount;

    SolrConfig() {
        // CDI required ctor
        this(null, null, null, 0, 0);
    }

    @Inject
    public SolrConfig(@PropertyValue(name = "Solr.ServerURL") String solrServerBaseURL,
                      @PropertyValue(name = "Solr.MainCore") String solrMainCore,
                      @PropertyValue(name = "Solr.BuildCore") String solrBuildCore,
                      @IntPropertyValue(name = "Solr.LoaderQueueSize", defaultValue = 100) int solrLoaderQueueSize,
                      @IntPropertyValue(name = "Solr.LoaderThreadCount", defaultValue = 2) int solrLoaderThreadCount) {
        this.solrServerBaseURL = solrServerBaseURL;
        this.solrMainCore = solrMainCore;
        this.solrBuildCore = solrBuildCore;
        this.solrLoaderQueueSize = solrLoaderQueueSize;
        this.solrLoaderThreadCount = solrLoaderThreadCount;
        if (StringUtils.isBlank(solrServerBaseURL)) {
            LOG.warn("No SOLR server configured (Solr.ServerURL)");
        }
        else {
            LOG.info("Use SOLR URL: {} with core {} and build core {}", solrServerBaseURL, solrMainCore, solrBuildCore);
        }
    }

    String getSolrServerBaseURL() {
        return solrServerBaseURL;
    }

    String getSolrMainCore() {
        return solrMainCore;
    }

    String getSolrBuildCore() {
        return solrBuildCore;
    }

    int getSolrLoaderQueueSize() {
        return solrLoaderQueueSize;
    }

    int getSolrLoaderThreadCount() {
        return solrLoaderThreadCount;
    }

    SolrBuilder builder() {
        return new SolrBuilder(this);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("solrServerBaseURL", solrServerBaseURL)
                .append("solrMainCore", solrMainCore)
                .append("solrBuildCore", solrBuildCore)
                .append("solrLoaderQueueSize", solrLoaderQueueSize)
                .append("solrLoaderThreadCount", solrLoaderThreadCount)
                .toString();
    }
}

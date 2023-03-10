package org.janelia.jacs2.dataservice.search;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(SolrBuilder.class);

    private final SolrConfig solrConfig;

    private String solrServerBaseURL;
    private String solrCore;
    private boolean concurrentUpdate;

    SolrBuilder(SolrConfig solrConfig) {
        this.solrConfig = solrConfig;
    }

    public SolrBuilder setSolrServerBaseURL(String solrServerBaseURL) {
        this.solrServerBaseURL = solrServerBaseURL;
        return this;
    }

    public SolrBuilder setSolrCore(String solrCore) {
        this.solrCore = solrCore;
        return this;
    }

    public SolrBuilder setConcurrentUpdate(boolean concurrentUpdate) {
        this.concurrentUpdate = concurrentUpdate;
        return this;
    }

    public SolrClient build() {
        String solrBaseURL ;
        if (StringUtils.isBlank(solrServerBaseURL)) {
            solrBaseURL = solrConfig.getSolrServerBaseURL();
        } else {
            solrBaseURL = solrServerBaseURL;
        }
        if (StringUtils.isBlank(solrBaseURL)) {
            return null;
        } else {
            String solrCoreName = StringUtils.defaultIfBlank(solrCore, "");
            String solrURL = StringUtils.appendIfMissing(solrBaseURL, "/") + solrCoreName;
            if (solrConfig.getSolrStallTimeMillis() > 0) {
                System.setProperty("solr.cloud.client.stallTime", String.valueOf(solrConfig.getSolrStallTimeMillis()));
            }
            if (concurrentUpdate) {
                try {
                    return new ConcurrentUpdateSolrClient.Builder(solrURL)
                            .withQueueSize(solrConfig.getSolrLoaderQueueSize())
                            .withThreadCount(solrConfig.getSolrLoaderThreadCount())
                            .withConnectionTimeout(solrConfig.getSolrConnectionTimeoutMillis())
                            .withSocketTimeout(solrConfig.getSolrSocketTimeoutMillis())
                            .neverStreamDeletes()
                            .build();
                } catch (Exception e) {
                    LOG.error("Error instantiating concurrent SOLR for {} with core {} -> {} and concurrent params: {}",
                            solrBaseURL, solrCoreName, solrURL, solrConfig);
                    throw new IllegalArgumentException("Error instantiating concurrent SOLR server for " + solrURL, e);
                }
            } else {
                return new HttpSolrClient.Builder(solrURL).build();
            }
        }
    }

}

package org.janelia.jacs2.dataservice.search;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(SolrBuilder.class);

    private final SolrConfig solrConfig;

    private String solrServerBaseURL;
    private String solrCore;
    private boolean concurrentUpdate;
    private int indexingQueueSize;
    private int indexingThreads;

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

    public SolrBuilder setIndexingQueueSize(int indexingQueueSize) {
        this.indexingQueueSize = indexingQueueSize;
        return this;
    }

    public SolrBuilder setIndexingThreads(int indexingThreads) {
        this.indexingThreads = indexingThreads;
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
            if (concurrentUpdate) {
                int queueSize;
                if (indexingQueueSize <= 0) {
                    queueSize = solrConfig.getSolrLoaderQueueSize();
                } else {
                    queueSize = indexingQueueSize;
                }
                int threadCount;
                if (indexingThreads <= 0) {
                    threadCount = solrConfig.getSolrLoaderThreadCount();
                } else {
                    threadCount = indexingThreads;
                }
                try {
                    return new ConcurrentUpdateSolrClient.Builder(solrURL)
                            .withQueueSize(queueSize)
                            .withThreadCount(threadCount)
                            .build();
                } catch (Exception e) {
                    LOG.error("Error instantiating concurrent SOLR for {} with core {} -> {} and concurrent params - queueSize: {}, threadCount: {}",
                            solrBaseURL, solrCoreName, solrURL, queueSize, threadCount);
                    throw new IllegalArgumentException("Error instantiating concurrent SOLR server for " + solrURL, e);
                }
            } else {
                return new HttpSolrClient.Builder(solrServerBaseURL).build();
            }
        }
    }

}

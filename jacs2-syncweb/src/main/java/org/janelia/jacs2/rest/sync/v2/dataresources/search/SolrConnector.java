package org.janelia.jacs2.rest.sync.v2.dataresources.search;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * A SOLR connector.
 * 
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SolrConnector {
	private static final Logger LOG = LoggerFactory.getLogger(SolrConnector.class);

	private final SolrServer solr;

	@Inject
	SolrConnector(@PropertyValue(name = "Solr.ServerURL") String solrURL) {
		this.solr = new HttpSolrServer(solrURL);
	}

	/**
	 * Run the given query against the index.
	 * @param query
	 * @return
	 * @throws DaoException
	 */
	public QueryResponse search(SolrQuery query) {
		LOG.trace("search(query={})", query.getQuery());

		LOG.debug("Running SOLR query: {}", query);
		try {
			return solr.query(query);
		} catch (SolrServerException e) {
			LOG.error("Search error for {}", query, e);
			throw new IllegalStateException(e);
		}
	}

}
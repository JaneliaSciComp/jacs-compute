package org.janelia.jacs2.dataservice.search;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.support.SearchAttribute;
import org.janelia.model.util.ReflectionHelper;
import org.reflections.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A SOLR connector.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SolrConnector {
    private static final Logger LOG = LoggerFactory.getLogger(SolrConnector.class);

    private final SolrServer solr;
    private final int solrCommitDelayInMillis;

    @Inject
    SolrConnector(@PropertyValue(name = "Solr.ServerURL") String solrURL,
                  @IntPropertyValue(name = "Solr.CommitDelayInMillis", defaultValue = 500) int solrCommitDelayInMillis) {
        this.solr = new HttpSolrServer(solrURL);
        this.solrCommitDelayInMillis = solrCommitDelayInMillis;
    }

    /**
     * Run the given query against the index.
     *
     * @param query
     * @return
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

    public boolean addToIndex(DomainObject domainObject) {
        try {
            solr.add(createSolrDoc(domainObject), solrCommitDelayInMillis);
            return true;
        } catch (Exception e) {
            LOG.error("Error while updating solr index for {}", domainObject, e);
            return false;
        }
    }

    public boolean removeFromIndexById(String id) {
        try {
            solr.deleteById(id, solrCommitDelayInMillis);
            return true;
        } catch (Exception e) {
            LOG.error("Error while removing {} from solr index", id, e);
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private SolrInputDocument createSolrDoc(DomainObject domainObject) {
        SolrInputDocument solrDoc = new SolrInputDocument();
        solrDoc.setField("doc_type", SolrDocType.DOCUMENT.name(), 1.0f);
        solrDoc.setField("class", domainObject.getClass().getName(), 1.0f);
        solrDoc.setField("collection", DomainUtils.getCollectionName(domainObject), 1.0f);

        Map<String, Object> attrs = new HashMap<>();
        Set<Field> searchableFields = ReflectionUtils.getAllFields(domainObject.getClass(), ReflectionUtils.withAnnotation(SearchAttribute.class));

        BiConsumer<SearchAttribute, Object> searchFieldHandler = (searchAttribute, fieldValue) -> {
            if (fieldValue == null || fieldValue instanceof String && StringUtils.isBlank((String) fieldValue)) {
                solrDoc.removeField(searchAttribute.key());
                if (StringUtils.isNotEmpty(searchAttribute.facet())) {
                    solrDoc.removeField(searchAttribute.facet());
                }
            } else {
                solrDoc.addField(searchAttribute.key(), fieldValue, 1.0f);
                if (StringUtils.isNotEmpty(searchAttribute.facet())) {
                    solrDoc.addField(searchAttribute.facet(), fieldValue, 1.0f);
                }
            }
        };
        for (Field field : searchableFields) {
            try {
                SearchAttribute searchAttributeAnnot = field.getAnnotation(SearchAttribute.class);
                Object value = ReflectionHelper.getFieldValue(domainObject, field.getName());
                searchFieldHandler.accept(searchAttributeAnnot, value);
            } catch (NoSuchFieldException e) {
                throw new IllegalArgumentException("No such field " + field.getName() + " on object " + domainObject, e);
            }
        }

        Set<Method> searchableProperties = ReflectionUtils.getAllMethods(domainObject.getClass(), ReflectionUtils.withAnnotation(SearchAttribute.class));
        for (Method propertyMethod : searchableProperties) {
            try {
                SearchAttribute searchAttributeAnnot = propertyMethod.getAnnotation(SearchAttribute.class);
                Object value = propertyMethod.invoke(domainObject);
                searchFieldHandler.accept(searchAttributeAnnot, value);
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw new IllegalArgumentException("Problem executing " + propertyMethod.getName() + " on object " + domainObject, e);
            }
        }

        return solrDoc;
    }
}
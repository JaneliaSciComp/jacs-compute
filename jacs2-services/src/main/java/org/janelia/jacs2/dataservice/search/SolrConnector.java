package org.janelia.jacs2.dataservice.search;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A SOLR connector.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SolrConnector {
    private static final Logger LOG = LoggerFactory.getLogger(SolrConnector.class);

    private final SolrServer solrServer;
    private final int solrCommitDelayInMillis;

    @Inject
    SolrConnector(@PropertyValue(name = "Solr.ServerURL") String solrURL,
                  @IntPropertyValue(name = "Solr.CommitDelayInMillis", defaultValue = 500) int solrCommitDelayInMillis) {
        this.solrServer = StringUtils.isBlank(solrURL) ? null : new HttpSolrServer(solrURL.trim());
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
        if (solrServer == null) {
            LOG.debug("SOLR search is not configured");
            return new QueryResponse();
        } else {
            LOG.debug("Running SOLR query: {}", query);
            try {
                return solrServer.query(query);
            } catch (SolrServerException e) {
                LOG.error("Search error for {}", query, e);
                throw new IllegalStateException(e);
            }
        }
    }

    public boolean addToIndex(DomainObject domainObject) {
        if (solrServer == null) {
            return false;
        } else {
            try {
                solrServer.add(createSolrDoc(domainObject), solrCommitDelayInMillis);
                solrServer.commit(false, false);
                return true;
            } catch (Exception e) {
                LOG.error("Error while updating solr index for {}", domainObject, e);
                return false;
            }
        }
    }

    public boolean removeFromIndexById(String id) {
        if (solrServer == null) {
            return false;
        } else {
            try {
                solrServer.deleteById(id, solrCommitDelayInMillis);
                solrServer.commit(false, false);
                return true;
            } catch (Exception e) {
                LOG.error("Error while removing {} from solr index", id, e);
                return false;
            }
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

    public void updateAncestorsForSolrDocs(List<Long> solrDocIds, Long ancestorDocId, int batchSize) {
        List<SolrInputDocument> solrDocs = searchByDocIds(solrDocIds, batchSize)
                .map(solrDoc -> ClientUtils.toSolrInputDocument(solrDoc))
                .map(solrInputDoc -> {
                    Collection<Long> ancestorIds;
                    SolrInputField field = solrInputDoc.getField("ancestor_ids");
                    if (field == null || field.getValue() == null) {
                        ancestorIds = new ArrayList<>();
                    } else {
                        ancestorIds = (Collection<Long>) field.getValue();
                    }
                    ancestorIds.add(ancestorDocId);
                    solrInputDoc.setField("ancestor_ids", ancestorIds, 0.2f);
                    return solrInputDoc;
                })
                .collect(Collectors.toList())
                ;
        if (CollectionUtils.isNotEmpty(solrDocs)) {
            try {
                solrServer.add(solrDocs, solrCommitDelayInMillis);
            } catch (Exception e) {
                LOG.error("Error persisting documents {} after trying to update ancestor id to {}", solrDocIds, ancestorDocId, e);
            }
        }
    }

    private Stream<SolrDocument> searchByDocIds(List<Long> solrDocIds, int batchSize) {
        if (solrServer == null) {
            LOG.debug("SOLR server is not setup");
            return Stream.of();
        }
        if (CollectionUtils.isEmpty(solrDocIds)) {
            return Stream.of();
        }
        final AtomicInteger counter = new AtomicInteger();
        Collection<List<Long>> solrDocIdsPartitions = batchSize > 0
                ? solrDocIds.stream().collect(Collectors.groupingBy(docId -> counter.getAndIncrement() / batchSize)).values()
                : Collections.singleton(solrDocIds);
        return solrDocIdsPartitions.stream()
                .map(partition -> partition.stream().map(id -> "id:" + id.toString()).reduce((id1, id2) -> id1 + " OR " + id2).orElse(null))
                .filter(queryStr -> queryStr != null)
                .map(SolrQuery::new)
                .map(this::search)
                .flatMap(queryResponse -> queryResponse.getResults().stream())
                ;
    }

}

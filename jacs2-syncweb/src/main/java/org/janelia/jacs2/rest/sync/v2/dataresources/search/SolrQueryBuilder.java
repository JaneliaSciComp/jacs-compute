package org.janelia.jacs2.rest.sync.v2.dataresources.search;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.janelia.jacs2.dataservice.search.SolrDocType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A helper class for building SOLR queries against the Entity model.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SolrQueryBuilder {

    private static final Logger log = LoggerFactory.getLogger(SolrQueryBuilder.class);
    private String searchString;
    private String auxString;
    private String auxAnnotationQueryString;
    private Long rootId;
    private List<String> ownerKeys = new ArrayList<>();
    private Map<String, Set<String>> filters = new HashMap<>();
    private List<String> facets = new ArrayList<>();
    private String sortField;
    private boolean ascending;

    public SolrQueryBuilder() {
    }

    public String getSearchString() {
        return searchString;
    }

    public void setSearchString(String searchString) {
        this.searchString = searchString;
    }

    public String getAuxString() {
        return auxString;
    }

    public void setAuxString(String auxString) {
        this.auxString = auxString;
    }

    public String getAuxAnnotationQueryString() {
        return auxAnnotationQueryString;
    }

    public void setAuxAnnotationQueryString(String auxAnnotationQueryString) {
        this.auxAnnotationQueryString = auxAnnotationQueryString;
    }

    public Long getRootId() {
        return rootId;
    }

    public void setRootId(Long rootId) {
        this.rootId = rootId;
    }

    public List<String> getOwnerKeys() {
        return ownerKeys;
    }

    public void addOwnerKey(String ownerKey) {
        this.ownerKeys.add(ownerKey);
    }

    public Map<String, Set<String>> getFilters() {
        return filters;
    }

    public void setFilters(Map<String, Set<String>> filters) {
        this.filters = filters;
    }

    public List<String> getFacets() {
        return facets;
    }

    public void setFacets(List<String> facets) {
        this.facets = facets;
    }

    public String getSortField() {
        return sortField;
    }

    public void setSortField(String sortField) {
        this.sortField = sortField;
    }

    public boolean isAscending() {
        return ascending;
    }

    public void setAscending(boolean ascending) {
        this.ascending = ascending;
    }

    public boolean hasQuery() {
        return !StringUtils.isEmpty(searchString) || !StringUtils.isEmpty(auxString) || !StringUtils.isEmpty(auxAnnotationQueryString) || rootId != null || !filters.isEmpty();
    }

    public SolrQuery getQuery() {
        StringBuilder qs = new StringBuilder();
        if (!ownerKeys.isEmpty()) {
            qs.append("+subjects:(");
            int i = 0;
            for (String ownerKey : ownerKeys) {
                if (i++ > 0) {
                    qs.append(" OR ");
                }
                String ownerName = ownerKey.split(":")[1];
                qs.append(ownerName);
            }
            qs.append(")");
        }

        if (rootId != null) {
            qs.append(" AND (ancestor_ids:").append(rootId).append(")");
        }

        if (!StringUtils.isEmpty(auxString)) {
            qs.append(" +(");
            qs.append(auxString);
            qs.append(")");
        }

        boolean outer = StringUtils.isNotBlank(searchString) || StringUtils.isNotBlank(auxAnnotationQueryString);
        if (outer) {
            qs.append(" +(");
        }

        if (StringUtils.isNotBlank(searchString)) {
            String escapedSearchString = searchString == null ? "" : searchString.replaceAll(":", "\\:");
            qs.append("(").append(escapedSearchString).append(")");
            if (StringUtils.isEmpty(auxAnnotationQueryString)) {
                qs.append(" OR ");
            } else {
                qs.append(" AND ");
            }
        }

        String annotationSearchString = getAnnotationSearchString();
        if (StringUtils.isNotBlank(annotationSearchString)) {
            qs.append("(");
            int i = 0;
            for (String ownerKey : ownerKeys) {
                String ownerName = ownerKey.split(":")[1];
                String fieldNamePrefix = SolrUtils.getFormattedName(ownerName);
                if (i++ > 0) {
                    qs.append(" OR ");
                }
                qs.append(fieldNamePrefix).append("_annotations:").append(annotationSearchString);
            }
            qs.append(")");
        }

        if (outer) {
            qs.append(")");
        }

        SolrQuery query = new SolrQuery();
        query.setQuery(qs.toString());
        query.addField("score");

        if (sortField != null) {
            query.setSortField(sortField, ascending ? ORDER.asc : ORDER.desc);
        }

        boolean entityTypeFiltered = false;
        for (String fieldName : filters.keySet()) {
            Set<String> values = filters.get(fieldName);
            if (values == null || values.isEmpty()) {
                continue;
            }
            query.addFilterQuery(getFilterQuery(fieldName, values));
            if ("type".equals(fieldName)) {
                entityTypeFiltered = true;
            }
        }

        query.addFilterQuery("+doc_type:" + SolrDocType.DOCUMENT.name());
        if (!entityTypeFiltered) {
            query.addFilterQuery("-type:Ontology*");
        }

        for (String facet : facets) {
            // Exclude the facet field from itself, to support multi-valued faceting
            query.addFacetField("{!ex=" + facet + "}" + facet);
        }

        return query;
    }

    private String getAnnotationSearchString() {
        StringBuilder query = new StringBuilder();
        String escapedSearchString;
        if (StringUtils.isEmpty(auxAnnotationQueryString)) {
            if (StringUtils.isNotEmpty(searchString)) {
                escapedSearchString = searchString.replaceAll(":", "\\:");
            } else {
                escapedSearchString = null;
            }
        } else {
            escapedSearchString = auxAnnotationQueryString.replaceAll(":", "\\:");
        }
        if (StringUtils.isNotEmpty(escapedSearchString)) {
            query.append("(");
            query.append(escapedSearchString);
            query.append(")");
        }
        return query.toString();
    }

    public static SolrParams serializeSolrQuery(SolrQuery query) {
        SolrParams queryParams = new SolrParams();
        queryParams.setQuery(query.getQuery());
        queryParams.setSortField(query.getSortField());
        queryParams.setFilterQueries(query.getFilterQueries());
        queryParams.setFacetField(query.getFacetFields());
        queryParams.setRows(query.getRows());
        queryParams.setStart(query.getStart());
        return queryParams;
    }

    public static SolrQuery deSerializeSolrQuery(SolrParams queryParams) {
        SolrQuery query = new SolrQuery();
        query.setQuery(queryParams.getQuery());
        if (queryParams.getSortField() != null) {
            String[] sortParams = queryParams.getSortField().split(" ");
            ORDER sortOrder = (sortParams[1].equals("asc") ? ORDER.asc : ORDER.desc);
            query.setSortField(sortParams[0], sortOrder);
        }
        query.setFilterQueries(queryParams.getFilterQueries());
        String[] facetFields = queryParams.getFacetField();
        for (int i = 0; i < facetFields.length; i++) {
            query.addFacetField(facetFields[i]);
        }
        query.setStart(queryParams.getStart());
        query.setRows(queryParams.getRows());
        return query;
    }

    /**
     * Returns a SOLR-style field query for the given field containing the given values. Also tags the
     * field so that it can be excluded in facets on other fields.
     *
     * @param fieldName
     * @param values
     * @return
     */
    protected String getFilterQuery(String fieldName, Set<String> values) {
        StringBuilder sb = new StringBuilder();
        sb.append("{!tag=").append(fieldName).append("}").append(fieldName);
        sb.append(":("); // Sad face :/
        for (String value : values) {
            sb.append("\"");
            sb.append(value);
            sb.append("\" ");
        }
        sb.append(")");
        return sb.toString();
    }
}

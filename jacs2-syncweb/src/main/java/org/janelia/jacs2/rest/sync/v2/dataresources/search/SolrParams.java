package org.janelia.jacs2.rest.sync.v2.dataresources.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by schauderd on 2/4/16.
 */
public class SolrParams {

    private String query;
    private String sortField;
    private String[] filterQueries;
    private String[] facetField;
    private int facetMinCount;
    private Integer start;
    private Integer rows;
    private List<String> fields = new ArrayList<String>();
    private String subjectKey;

    public SolrParams() {
        facetMinCount = 1;
        fields.add("score");
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getSortField() {
        return sortField;
    }

    public void setSortField(String sortField) {
        this.sortField = sortField;
    }

    public String[] getFilterQueries() {
        return filterQueries;
    }

    public void setFilterQueries(String[] filterQueries) {
        this.filterQueries = filterQueries;
    }

    public String[] getFacetField() {
        return facetField;
    }

    public void setFacetField(String[] facetField) {
        this.facetField = facetField;
    }

    public Integer getStart() {
        return start;
    }

    public void setStart(Integer start) {
        this.start = start;
    }

    public Integer getRows() {
        return rows;
    }

    public void setRows(Integer rows) {
        this.rows = rows;
    }

    public String getSubjectKey(){return subjectKey;}

    public void setSubjectKey(String subjectKey) {
        this.subjectKey = subjectKey;
    }

    @Override
    public String toString() {
        return "SolrParams [query=" + query + ", sortField=" + sortField + ", filterQueries=" + Arrays.toString(filterQueries) + ", facetField="
                + Arrays.toString(facetField) + ", facetMinCount=" + facetMinCount + ", start=" + start + ", rows=" + rows + ", fields=" + fields+ ", subjectKey=" + subjectKey
                + "]";
    }
}

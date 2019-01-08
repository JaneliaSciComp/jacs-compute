package org.janelia.jacs2.rest.sync.v2.dataresources.search;

/**
 * The count for a facet value.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class FacetValue {
    private String value;
    private long count;

    public FacetValue() {
    }

    public FacetValue(String value, long count) {
        this.value = value;
        this.count = count;
    }


    public void setValue(String value) {
        this.value = value;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getValue() {
        return value;
    }

    public long getCount() {
        return count;
    }
}
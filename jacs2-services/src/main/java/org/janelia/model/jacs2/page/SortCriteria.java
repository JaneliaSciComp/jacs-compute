package org.janelia.model.jacs2.page;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class SortCriteria {
    private String field;
    private SortDirection direction;

    public SortCriteria() {
        this(null, SortDirection.ASC);
    }

    public SortCriteria(String field) {
        this(field, SortDirection.ASC);
    }

    public SortCriteria(String field, SortDirection direction) {
        this.field = field;
        this.direction = direction;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public SortDirection getDirection() {
        return direction;
    }

    public void setDirection(SortDirection direction) {
        this.direction = direction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        SortCriteria that = (SortCriteria) o;

        return new EqualsBuilder()
                .append(field, that.field)
                .append(direction, that.direction)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(field)
                .append(direction)
                .toHashCode();
    }
}

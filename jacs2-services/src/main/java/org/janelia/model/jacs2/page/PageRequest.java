package org.janelia.model.jacs2.page;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.List;

public class PageRequest {
    private long firstPageOffset;
    private long pageNumber;
    private int pageSize;
    private List<SortCriteria> sortCriteria;

    public long getFirstPageOffset() {
        return firstPageOffset;
    }

    public void setFirstPageOffset(long firstPageOffset) {
        this.firstPageOffset = firstPageOffset;
    }

    public long getPageNumber() {
        return pageNumber;
    }

    public void setPageNumber(long pageNumber) {
        this.pageNumber = pageNumber;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public List<SortCriteria> getSortCriteria() {
        return sortCriteria;
    }

    public void setSortCriteria(List<SortCriteria> sortCriteria) {
        this.sortCriteria = sortCriteria;
    }

    public long getOffset() {
        long offset = 0L;
        if (firstPageOffset > 0) {
            offset = firstPageOffset;
        }
        if (pageNumber > 0 && pageSize > 0) {
            offset += pageNumber * pageSize;
        }
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        PageRequest that = (PageRequest) o;

        return new EqualsBuilder()
                .append(firstPageOffset, that.firstPageOffset)
                .append(pageNumber, that.pageNumber)
                .append(pageSize, that.pageSize)
                .append(sortCriteria, that.sortCriteria)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(firstPageOffset)
                .append(pageNumber)
                .append(pageSize)
                .append(sortCriteria)
                .toHashCode();
    }
}

package org.janelia.it.jacs.model.domain;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

/**
 * A reverse reference to a set of DomainObjects in another collection. The referring DomainObjects
 * each have type <i>referringCollectionName</i> and contain an attribute with name <i>referenceAttr</i> and
 * value <i>referenceId</i>.
 */
public class ReverseReference implements Serializable {

    private String referringClassName;
    private String referenceAttr;
    private Number referenceId;
    private Long count;

    public String getReferringClassName() {
        return referringClassName;
    }

    public void setReferringClassName(String referringClassName) {
        this.referringClassName = referringClassName;
    }

    public String getReferenceAttr() {
        return referenceAttr;
    }

    public void setReferenceAttr(String referenceAttr) {
        this.referenceAttr = referenceAttr;
    }

    public Number getReferenceId() {
        return referenceId;
    }

    public void setReferenceId(Number referenceId) {
        this.referenceId = referenceId;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("referringClassName", referringClassName)
                .append("referenceAttr", referenceAttr)
                .append("referenceId", referenceId)
                .append("count", count)
                .build();
    }
}

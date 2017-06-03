package org.janelia.it.jacs.model.domain;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * A reference to an object with an positional index which can be the corresponding index into some collection.
 */
public class IndexedReference<T, I> {
    private final T reference;
    private final I pos;

    public IndexedReference(T reference, I pos) {
        this.reference = reference;
        this.pos = pos;
    }

    public T getReference() {
        return reference;
    }

    public I getPos() {
        return pos;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        IndexedReference<?, ?> that = (IndexedReference<?, ?>) o;

        return new EqualsBuilder()
                .append(pos, that.pos)
                .append(reference, that.reference)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(reference)
                .append(pos)
                .toHashCode();
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}

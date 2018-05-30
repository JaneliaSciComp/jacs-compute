package org.janelia.model.jacs2.domain;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A reference to an object with an positional index which can be the corresponding index into some collection.
 * @param <T> the type of the encapsulated object.
 * @param <I> the type of the indexing object.
 */
public class IndexedReference<T, I> {

    public static <T, I> Stream<IndexedReference<T, I>> indexListContent(List<T> content, BiFunction<Integer, T, IndexedReference<T, I>> indexedMapper) {
        return IntStream.range(0, content.size())
                .mapToObj(pos -> indexedMapper.apply(pos, content.get(pos)));
    }

    public static <T, I> Stream<IndexedReference<T, I>> indexArrayContent(T[] content, BiFunction<Integer, T, IndexedReference<T, I>> indexedMapper) {
        return IntStream.range(0, content.length)
                .mapToObj(pos -> indexedMapper.apply(pos, content[pos]));
    }

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

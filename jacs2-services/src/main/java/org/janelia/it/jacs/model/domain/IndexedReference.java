package org.janelia.it.jacs.model.domain;

/**
 * A reference to an object with an positional index which can be the corresponding index into some collection.
 */
public class IndexedReference<T> {
    private final T reference;
    private final int pos;

    public IndexedReference(T reference, int pos) {
        this.reference = reference;
        this.pos = pos;
    }

    public T getReference() {
        return reference;
    }

    public int getPos() {
        return pos;
    }
}

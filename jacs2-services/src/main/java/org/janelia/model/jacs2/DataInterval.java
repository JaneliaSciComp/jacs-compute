package org.janelia.model.jacs2;

public class DataInterval<T> {
    private final T from, to;

    public DataInterval(T from, T to) {
        this.from = from;
        this.to = to;
    }

    public boolean hasFrom() {
        return from != null;
    }

    public boolean hasTo() {
        return to != null;
    }

    public T getFrom() {
        return from;
    }

    public T getTo() {
        return to;
    }
}

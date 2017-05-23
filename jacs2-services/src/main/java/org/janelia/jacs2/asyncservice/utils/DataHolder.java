package org.janelia.jacs2.asyncservice.utils;

import javax.enterprise.inject.Vetoed;

@Vetoed
public class DataHolder<T> {
    private T data;

    public DataHolder() {
    }

    public DataHolder(T data) {
        this.data = data;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public boolean isPresent() {
        return data != null;
    }
}

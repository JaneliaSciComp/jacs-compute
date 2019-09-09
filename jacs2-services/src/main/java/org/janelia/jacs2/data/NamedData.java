package org.janelia.jacs2.data;

public class NamedData<D> {

    private final String name;
    private final D data;

    public NamedData(String name, D data) {
        this.name = name;
        this.data = data;
    }

    public String getName() {
        return name;
    }

    public D getData() {
        return data;
    }
}

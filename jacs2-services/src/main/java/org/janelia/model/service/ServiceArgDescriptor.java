package org.janelia.model.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ServiceArgDescriptor {

    private final List<String> names;
    private final Object defaultValue;
    private final int arity;
    private final boolean required;
    private final String description;

    public ServiceArgDescriptor(String[] names,
                                Object defaultValue,
                                int arity,
                                boolean required,
                                String description) {
        this.names = Arrays.asList(names);
        this.defaultValue = defaultValue;
        this.arity = arity;
        this.required = required;
        this.description = description;
    }

    public List<String> getNames() {
        return names;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public int getArity() {
        return arity;
    }

    public boolean isRequired() {
        return required;
    }

    public String getDescription() {
        return description;
    }
}

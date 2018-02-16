package org.janelia.model.service;

import com.beust.jcommander.Parameterized;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Arrays;
import java.util.List;

public class ServiceArgDescriptor {

    @JsonIgnore
    private final Parameterized arg;
    private final List<String> cmdFlags;
    private final Object defaultValue;
    private final int arity;
    private final boolean required;
    private final String description;

    public ServiceArgDescriptor(Parameterized arg,
                                String[] cmdFlags,
                                Object defaultValue,
                                int arity,
                                boolean required,
                                String description) {
        this.arg = arg;
        this.cmdFlags = Arrays.asList(cmdFlags);
        this.defaultValue = defaultValue;
        this.arity = arity;
        this.required = required;
        this.description = description;
    }

    public Parameterized getArg() {
        return arg;
    }

    public String getArgName() {
        return arg.getName();
    }

    public List<String> getCmdFlags() {
        return cmdFlags;
    }

    public String getArgType() {
        return arg.getGenericType().getTypeName();
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

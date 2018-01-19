package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Arrays;
import java.util.List;

public class ServiceArg {
    private final String flag;
    private final int arity;
    private final String[] values;

    public ServiceArg(String flag) {
        this.flag = flag;
        this.arity = 0;
        this.values = new String[0];
    }

    public ServiceArg(String flag, String value) {
        if (StringUtils.isBlank(value)) {
            this.flag = null;
            this.arity = 0;
            this.values = null;
        } else {
            this.flag = flag;
            this.arity = 1;
            this.values = new String[] {value};
        }
    }

    public ServiceArg(String flag, Number numericValue) {
        if (numericValue == null) {
            this.flag = null;
            this.arity = 0;
            this.values = null;
        } else {
            this.flag = flag;
            this.arity = 1;
            this.values = new String[] {numericValue.toString()};
        }
    }

    public ServiceArg(String flag, String value1, String... values) {
        this.flag = flag;
        this.arity = values.length + 1;
        this.values = new String[values.length + 1];
        this.values[0] = value1;
        System.arraycopy(values, 0, this.values, 1, values.length);
    }

    public ServiceArg(String flag, List<String> values) {
        this.flag = flag;
        this.arity = values.size();
        String[] valueArr = new String[values.size()];
        this.values = values.toArray(valueArr);
    }

    public ServiceArg(String flag, boolean value) {
        if (value) {
            this.flag = flag;
            this.arity = 0;
            this.values = new String[0];
        } else {
            this.flag = null;
            this.arity = 0;
            this.values = null;
        }
    }

    public ServiceArg(String flag, int value) {
        this.flag = flag;
        this.arity = 1;
        this.values = new String[] { String.valueOf(value) };
    }

    public String[] toStringArray() {
        if (flag == null) {
            return new String[]{};
        } else {
            int nargs = arity > values.length ? values.length : arity;
            String[] args;
            int valuesStartIndex = 0;
            if (StringUtils.isBlank(flag)) {
                args = new String[nargs];
                valuesStartIndex = 0;
            } else {
                args = new String[nargs + 1];
                args[0] = flag;
                valuesStartIndex = 1;
            }
            for (int i = 0; i < arity; i++) {
                args[i + valuesStartIndex] = values[i];
            }
            return args;
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append(this.toStringArray()).build();
    }
}

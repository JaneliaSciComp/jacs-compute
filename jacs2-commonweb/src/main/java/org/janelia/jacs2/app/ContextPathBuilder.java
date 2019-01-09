package org.janelia.jacs2.app;

import org.apache.commons.lang3.StringUtils;

public class ContextPathBuilder {

    private final StringBuilder pathBuilder = new StringBuilder();

    public ContextPathBuilder path(String path) {
        if (StringUtils.isNotBlank(path)) {
            pathBuilder.append(StringUtils.prependIfMissing(path, "/"));
        }
        return this;
    }

    public String build() {
        return pathBuilder.toString();
    }
}

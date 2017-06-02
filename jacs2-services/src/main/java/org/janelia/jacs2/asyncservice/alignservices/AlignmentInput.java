package org.janelia.jacs2.asyncservice.alignservices;

import org.apache.commons.lang3.StringUtils;

public class AlignmentInput {
    public String name;
    public String channels;
    public String ref;
    public String res;
    public String dims;

    public boolean isEmpty() {
        return StringUtils.isBlank(name);
    }
}

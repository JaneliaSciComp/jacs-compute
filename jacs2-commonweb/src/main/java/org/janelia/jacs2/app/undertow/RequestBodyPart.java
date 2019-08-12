package org.janelia.jacs2.app.undertow;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

class RequestBodyPart {
    String partMimeType;
    StringBuilder partBodyBuilder = new StringBuilder();

    RequestBodyPart(String partMimeType) {
        this.partMimeType = partMimeType;
    }

    @Override
    public String toString() {
        return partMimeType + "\n" + partBodyBuilder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        RequestBodyPart that = (RequestBodyPart) o;

        return new EqualsBuilder()
                .append(partMimeType, that.partMimeType)
                .append(partBodyBuilder.toString(), that.partBodyBuilder.toString())
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(partMimeType)
                .append(partBodyBuilder)
                .toHashCode();
    }
}

package org.janelia.jacs2.asyncservice.containerizedservices;

import com.beust.jcommander.SubParameter;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

class BindPath {
    @SubParameter(order = 0)
    String srcPath; // path outside the container
    @SubParameter(order = 1)
    String targetPath; // path inside the container
    @SubParameter(order = 2)
    String mountOpts; // mount options

    boolean isEmpty() {
        return StringUtils.isEmpty(srcPath);
    }

    boolean isNotEmpty() {
        return StringUtils.isNotEmpty(srcPath);
    }

    BindPath setSrcPath(String srcPath) {
        this.srcPath = srcPath;
        return this;
    }

    String asString() {
        StringBuilder specBuilder = new StringBuilder();
        specBuilder.append(srcPath);
        if (StringUtils.isNotBlank(targetPath) || StringUtils.isNotBlank(mountOpts)) {
            specBuilder.append(':');
            specBuilder.append(targetPath);
            if (StringUtils.isNotBlank(mountOpts)) {
                specBuilder.append(':');
                specBuilder.append(mountOpts);
            }
        }
        return specBuilder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        BindPath bindPath = (BindPath) o;

        return new EqualsBuilder()
                .append(srcPath, bindPath.srcPath)
                .append(targetPath, bindPath.targetPath)
                .append(mountOpts, bindPath.mountOpts)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(srcPath)
                .append(targetPath)
                .append(mountOpts)
                .toHashCode();
    }

    @Override
    public String toString() {
        return asString();
    }
}

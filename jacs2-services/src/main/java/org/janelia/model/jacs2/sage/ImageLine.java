package org.janelia.model.jacs2.sage;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public final class ImageLine {
    private final String lab;
    private final String dataset;
    private final String name;

    public ImageLine(String lab, String dataset, String name) {
        this.lab = lab;
        this.dataset = dataset;
        this.name = name;
    }

    public String getLab() {
        return lab;
    }

    public String getDataset() {
        return dataset;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        ImageLine imageLine = (ImageLine) o;

        return new EqualsBuilder()
                .append(lab, imageLine.lab)
                .append(dataset, imageLine.dataset)
                .append(name, imageLine.name)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(lab)
                .append(dataset)
                .append(name)
                .toHashCode();
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}

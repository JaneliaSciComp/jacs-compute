package org.janelia.it.jacs.model.domain.sample;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class SampleTileKey {

    private final String tileName;
    private final String area;

    public SampleTileKey(String tileName, String area) {
        this.tileName = tileName;
        this.area = area;
    }

    public String getTileName() {
        return tileName;
    }

    public String getArea() {
        return area;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        SampleTileKey tileKey = (SampleTileKey) o;

        return new EqualsBuilder()
                .append(tileName, tileKey.tileName)
                .append(area, tileKey.area)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(tileName)
                .append(area)
                .toHashCode();
    }

}

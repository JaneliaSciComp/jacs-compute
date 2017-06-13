package org.janelia.jacs2.asyncservice.imageservices;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class FijiColor {
    private char code;
    private int divisor;

    FijiColor(char code, int divisor) {
        this.code = code;
        this.divisor = divisor;
    }

    public char getCode() {
        return code;
    }

    public int getDivisor() {
        return divisor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        FijiColor fijiColor = (FijiColor) o;

        return new EqualsBuilder()
                .append(code, fijiColor.code)
                .append(divisor, fijiColor.divisor)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(code)
                .append(divisor)
                .toHashCode();
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}

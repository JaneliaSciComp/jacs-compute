package org.janelia.jacs2.asyncservice.imageservices.tools;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class ChannelComponents {
    public String channelSpec;
    public String referenceChannelsPos;
    public String signalChannelsPos;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        ChannelComponents that = (ChannelComponents) o;

        return new EqualsBuilder()
                .append(channelSpec, that.channelSpec)
                .append(referenceChannelsPos, that.referenceChannelsPos)
                .append(signalChannelsPos, that.signalChannelsPos)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(channelSpec)
                .append(referenceChannelsPos)
                .append(signalChannelsPos)
                .toHashCode();
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}

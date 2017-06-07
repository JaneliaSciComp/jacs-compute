package org.janelia.jacs2.asyncservice.imageservices.tools;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class ChannelComponents {
    public String channelSpec;
    /**
     * reference channel position which is the same as the zero based reference channel index
      */
    public String referenceChannelsPos;
    /**
     * signal channel position which is the same as to the zero based signal channel index
     */
    public String signalChannelsPos;
    /**
     * reference channel number - same as the one based channel number.
      */
    public String referenceChannelNumbers;

    public int getNumChannels() {
        return StringUtils.length(channelSpec);
    }

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

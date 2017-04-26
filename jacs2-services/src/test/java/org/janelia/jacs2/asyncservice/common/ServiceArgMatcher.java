package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.mockito.ArgumentMatcher;

public class ServiceArgMatcher implements ArgumentMatcher<ServiceArg> {

    private ServiceArg matcher;

    public ServiceArgMatcher(ServiceArg matcher) {
        this.matcher = matcher;
    }

    @Override
    public boolean matches(ServiceArg argument) {
        return new EqualsBuilder().append(matcher.toStringArray(), argument.toStringArray()).build();
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(matcher.toStringArray());
    }
}

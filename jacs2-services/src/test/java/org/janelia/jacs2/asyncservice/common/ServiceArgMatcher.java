package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.lang3.builder.EqualsBuilder;
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
}

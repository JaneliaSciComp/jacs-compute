package org.janelia.jacs2.testhelpers;

import com.google.common.collect.Streams;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.mockito.ArgumentMatcher;

import java.util.Arrays;
import java.util.List;

public class ListArgMatcher<T> implements ArgumentMatcher<List<T>> {
    private final List<ArgumentMatcher<T>> itemMatchers;

    public ListArgMatcher(List<ArgumentMatcher<T>> itemMatchers) {
        this.itemMatchers = itemMatchers;
    }

    @Override
    public boolean matches(List<T> argument) {
        return argument.size() == itemMatchers.size() &&
                Streams.zip(itemMatchers.stream(), argument.stream(), (itemMatcher, item) -> itemMatcher.matches(item))
                .reduce((r1, r2) -> r1 && r2)
                .orElse(false)
                ;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("itemMatchers", itemMatchers)
                .toString();
    }
}

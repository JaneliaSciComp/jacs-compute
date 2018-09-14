package org.janelia.jacs2.asyncservice.common;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ServiceArgsTest {

    private static class TestServiceArgs extends ServiceArgs {
        @Parameter(names = "-listArgsWithSplitter", splitter = ServiceArgSplitter.class)
        List<String> listArgsWithSplitter = new ArrayList<>();
    }

    @Test
    public void serviceArgsSplitting() {
        class TestData {
            private final String[] unparsedArgs;
            private final List<String> expectedArgsWithCustomSplitter;
            private final List<String> expectedPositionalArgs;

            private TestData(String[] unparsedArgs, List<String> expectedArgsWithCustomSplitter, List<String> expectedPositionalArgs) {
                this.unparsedArgs = unparsedArgs;
                this.expectedArgsWithCustomSplitter = expectedArgsWithCustomSplitter;
                this.expectedPositionalArgs = expectedPositionalArgs;
            }

            @Override
            public String toString() {
                return new ToStringBuilder(this)
                        .append("unparsedArgs", unparsedArgs)
                        .toString();
            }
        }

        TestData[] testData = new TestData[] {
                new TestData(
                        new String[] {
                        },
                        ImmutableList.of(),
                        ImmutableList.of()
                ),
                new TestData(
                        new String[] {
                                "-listArgsWithSplitter", "a1.1, a2.1, a3.1"
                        },
                        ImmutableList.of("a1.1", "a2.1", "a3.1"),
                        ImmutableList.of()
                ),
                new TestData(
                        new String[] {
                                "-listArgsWithSplitter", "a1.2, 'a2.2, a3.2', a4.2",
                                "-listArgsWithSplitter", "a5.2, 'a6.2, a7.2', \\'a8.2"
                        },
                        ImmutableList.of("a1.2", "a2.2, a3.2", "a4.2", "a5.2", "a6.2, a7.2", "'a8.2"),
                        ImmutableList.of()
                ),
                new TestData(
                        new String[] {
                                "-listArgsWithSplitter", "'a1.3, a2.3, a3.3, a4.3"
                        },
                        ImmutableList.of("a1.3, a2.3, a3.3, a4.3"),
                        ImmutableList.of()
                ),
                new TestData(
                        new String[] {
                                "-listArgsWithSplitter", "a1.4, 'a2.4, \\'a3.4, a4.4'"
                        },
                        ImmutableList.of("a1.4", "a2.4, 'a3.4, a4.4"),
                        ImmutableList.of()
                ),
                new TestData(
                        new String[] {
                                "-listArgsWithSplitter", "a1.5, 'a2.5', \"a3.5, a4.5"
                        },
                        ImmutableList.of("a1.5", "a2.5", "\"a3.5", "a4.5"),
                        ImmutableList.of()
                ),
                new TestData(
                        new String[] {
                                "-listArgsWithSplitter", "a1.6 \\'a2.6, a3.6, a4.6"
                        },
                        ImmutableList.of("a1.6 'a2.6", "a3.6", "a4.6"),
                        ImmutableList.of()
                ),
                new TestData(
                        new String[] {
                                "-listArgsWithSplitter", "a1.7 'a2.7', a3.7, a4.7"
                        },
                        ImmutableList.of("a1.7 a2.7", "a3.7", "a4.7"),
                        ImmutableList.of()
                ),
                new TestData(
                        new String[] {
                                "-listArgsWithSplitter", "a1.8 'a2.8, next_a2.8' next_a1.8, a3.8, a4.8",
                                "a1", "'a2, a2.1'", "a3, a4"
                        },
                        ImmutableList.of("a1.8 a2.8, next_a2.8 next_a1.8", "a3.8", "a4.8"),
                        ImmutableList.of("a1", "'a2, a2.1'", "a3, a4")
                )
        };
        for (TestData td : testData) {
            TestServiceArgs serviceArgs = ServiceArgs.parse(td.unparsedArgs, new TestServiceArgs());
            assertEquals("Splitted args " + td.toString(), td.expectedArgsWithCustomSplitter, serviceArgs.listArgsWithSplitter);
            assertEquals("Positional args " + td.toString(), td.expectedPositionalArgs, serviceArgs.getRemainingArgs());
        }
    }
}

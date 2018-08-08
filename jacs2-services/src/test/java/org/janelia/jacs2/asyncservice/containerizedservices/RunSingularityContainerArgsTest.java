package org.janelia.jacs2.asyncservice.containerizedservices;

import com.beust.jcommander.JCommander;
import com.google.common.collect.ImmutableList;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertThat;

public class RunSingularityContainerArgsTest {

    @Test
    public void bindPathArgs() {
        class TestData {
            private final String[] sargs;
            private final Matcher<RunSingularityContainerArgs> checker;

            TestData(String[] sargs, Matcher<RunSingularityContainerArgs> checker) {
                this.sargs = sargs;
                this.checker = checker;
            }
        }
        TestData[] testData = new TestData[] {
                new TestData(
                        new String[]{
                                "-containerLocation", "shub://location",
                                "-bindPaths", "/op1"
                        },
                        new TypeSafeMatcher<RunSingularityContainerArgs>() {
                            @Override
                            protected boolean matchesSafely(RunSingularityContainerArgs item) {
                                return "shub://location".equals(item.containerLocation) &&
                                        item.bindPaths.size() == 1 &&
                                        "/op1".equals(item.bindPaths.get(0).asString());
                            }
                            @Override
                            public void describeTo(Description description) {
                            }
                        }
                ),
                new TestData(
                        new String[] {
                                "-containerLocation", "shub://location",
                                "-bindPaths", "/op1:/ip1"
                        },
                        new TypeSafeMatcher<RunSingularityContainerArgs>() {
                            @Override
                            protected boolean matchesSafely(RunSingularityContainerArgs item) {
                                return "shub://location".equals(item.containerLocation) &&
                                        item.bindPaths.size() == 1 &&
                                        "/op1:/ip1".equals(item.bindPaths.get(0).asString());
                            }
                            @Override
                            public void describeTo(Description description) {
                                description.appendText("Expected containerLocation -> shub://location, bindPaths -> /op1:/ip1");
                            }
                        }
                ),
                new TestData(
                        new String[] {
                                "-containerLocation", "shub://location",
                                "-bindPaths", "/op1:/ip1:rw,/op2:/ip2:ro,/op3"
                        },
                        new TypeSafeMatcher<RunSingularityContainerArgs>() {
                            @Override
                            protected boolean matchesSafely(RunSingularityContainerArgs item) {
                                return "shub://location".equals(item.containerLocation) &&
                                        item.bindPaths.size() == 3 &&
                                        "/op1:/ip1:rw".equals(item.bindPaths.get(0).asString()) &&
                                        "/op2:/ip2:ro".equals(item.bindPaths.get(1).asString()) &&
                                        "/op3".equals(item.bindPaths.get(2).asString());
                            }
                            @Override
                            public void describeTo(Description description) {
                                description.appendText("Expected containerLocation -> shub://location, bindPaths -> /op1:/ip1:rw,/op2:/ip2:ro,/op3");
                            }
                        }
                ),
                new TestData(
                        new String[] {
                                "-containerLocation", "shub://location",
                                "-bindPaths", "/op1::rw,/op2::ro,/op3::"
                        },
                        new TypeSafeMatcher<RunSingularityContainerArgs>() {
                            @Override
                            protected boolean matchesSafely(RunSingularityContainerArgs item) {
                                return "shub://location".equals(item.containerLocation) &&
                                        item.bindPaths.size() == 3 &&
                                        "/op1::rw".equals(item.bindPaths.get(0).asString()) &&
                                        "/op2::ro".equals(item.bindPaths.get(1).asString()) &&
                                        "/op3".equals(item.bindPaths.get(2).asString());
                            }
                            @Override
                            public void describeTo(Description description) {
                                description.appendText("Expected containerLocation -> shub://location, bindPaths -> /op1::rw,/op2::ro,/op3");
                            }
                        }
                ),
                new TestData(
                        new String[] {
                                "-containerLocation", "shub://location",
                                "-bindPaths", "",
                                "other1", "other2"
                        },
                        new TypeSafeMatcher<RunSingularityContainerArgs>() {
                            @Override
                            protected boolean matchesSafely(RunSingularityContainerArgs item) {
                                return "shub://location".equals(item.containerLocation) &&
                                        item.bindPaths.size() == 1 && item.bindPaths.get(0).isEmpty() &&
                                        item.appArgs.size() == 0 &&
                                        ImmutableList.of("other1", "other2").equals(item.otherArgs);
                            }
                            @Override
                            public void describeTo(Description description) {
                                description.appendText("Expected containerLocation -> shub://location, bindPaths -> <empty>, otherArgs -> other1, other2");
                            }
                        }
                )
        };
        for (TestData td : testData) {
            RunSingularityContainerArgs args = new RunSingularityContainerArgs();
            JCommander.newBuilder()
                    .addObject(args)
                    .args(td.sargs)
                    .build();
            assertThat(Arrays.asList(td.sargs).toString(), args, td.checker);
        }

    }
}

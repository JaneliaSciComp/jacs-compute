package org.janelia.jacs2.asyncservice.containerizedservices;

import java.util.Arrays;

import com.beust.jcommander.JCommander;
import com.google.common.collect.ImmutableList;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

public class SingularityRunContainerArgsTest {

    @Test
    public void bindPathArgs() {
        class TestData {
            private final String[] sargs;
            private final Matcher<SingularityRunContainerArgs> checker;

            private TestData(String[] sargs, Matcher<SingularityRunContainerArgs> checker) {
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
                        new TypeSafeMatcher<SingularityRunContainerArgs>() {
                            @Override
                            protected boolean matchesSafely(SingularityRunContainerArgs item) {
                                return "shub://location".equals(item.containerLocation) &&
                                        item.bindPaths.size() == 1 &&
                                        "/op1".equals(item.bindPaths.get(0).asString(false));
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
                        new TypeSafeMatcher<SingularityRunContainerArgs>() {
                            @Override
                            protected boolean matchesSafely(SingularityRunContainerArgs item) {
                                return "shub://location".equals(item.containerLocation) &&
                                        item.bindPaths.size() == 1 &&
                                        "/op1:/ip1".equals(item.bindPaths.get(0).asString(false));
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
                        new TypeSafeMatcher<SingularityRunContainerArgs>() {
                            @Override
                            protected boolean matchesSafely(SingularityRunContainerArgs item) {
                                return "shub://location".equals(item.containerLocation) &&
                                        item.bindPaths.size() == 3 &&
                                        "/op1:/ip1:rw".equals(item.bindPaths.get(0).asString(false)) &&
                                        "/op2:/ip2:ro".equals(item.bindPaths.get(1).asString(false)) &&
                                        "/op3".equals(item.bindPaths.get(2).asString(false));
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
                        new TypeSafeMatcher<SingularityRunContainerArgs>() {
                            @Override
                            protected boolean matchesSafely(SingularityRunContainerArgs item) {
                                return "shub://location".equals(item.containerLocation) &&
                                        item.bindPaths.size() == 3 &&
                                        "/op1:/op1:rw".equals(item.bindPaths.get(0).asString(false)) &&
                                        "/op2:/op2:ro".equals(item.bindPaths.get(1).asString(false)) &&
                                        "/op3".equals(item.bindPaths.get(2).asString(false));
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
                        new TypeSafeMatcher<SingularityRunContainerArgs>() {
                            @Override
                            protected boolean matchesSafely(SingularityRunContainerArgs item) {
                                return "shub://location".equals(item.containerLocation) &&
                                        item.bindPaths.size() == 1 && item.bindPaths.get(0).isEmpty() &&
                                        item.appArgs.size() == 0 &&
                                        ImmutableList.of("other1", "other2").equals(item.getRemainingArgs());
                            }
                            @Override
                            public void describeTo(Description description) {
                                description.appendText("Expected containerLocation -> shub://location, bindPaths -> <empty>, otherArgs -> other1, other2");
                            }
                        }
                ),
                new TestData(
                        new String[] {
                                "-containerLocation", "shub://location",
                                "other1,other2", "other3", "other4,other5"
                        },
                        new TypeSafeMatcher<SingularityRunContainerArgs>() {
                            @Override
                            protected boolean matchesSafely(SingularityRunContainerArgs item) {
                                return "shub://location".equals(item.containerLocation) &&
                                        item.bindPaths.size() == 0 &&
                                        item.appArgs.size() == 0 &&
                                        ImmutableList.of("other1,other2", "other3", "other4,other5").equals(item.getRemainingArgs());
                            }
                            @Override
                            public void describeTo(Description description) {
                                description.appendText("Expected containerLocation -> shub://location, bindPaths -> <empty>, otherArgs -> other1, other2");
                            }
                        }
                )
        };
        for (TestData td : testData) {
            SingularityRunContainerArgs args = new SingularityRunContainerArgs();
            JCommander.newBuilder()
                    .addObject(args)
                    .args(td.sargs)
                    .build();
            MatcherAssert.assertThat(Arrays.asList(td.sargs).toString(), args, td.checker);
        }

    }
}

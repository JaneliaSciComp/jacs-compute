package org.janelia.jacs2.asyncservice.imageservices;

import com.google.common.collect.ImmutableList;

import org.hamcrest.MatcherAssert;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;

public class FijiUtilsTest {

    @Test
    public void defaultColorSpecAsString() {
        class TestData {
            final String chanSpec;
            final String signalColors;
            final char referenceColor;
            final String expectedColorSpec;

            public TestData(String chanSpec, String signalColors, char referenceColor, String expectedColorSpec) {
                this.chanSpec = chanSpec;
                this.signalColors = signalColors;
                this.referenceColor = referenceColor;
                this.expectedColorSpec = expectedColorSpec;
            }
        }

        List<TestData> testData = ImmutableList.of(
                new TestData("sssr", "RGB", '1', "RGB1"),
                new TestData("sr", "RGB", '1', "R1"),
                new TestData("sr", "G", 'M', "GM"),
                new TestData("sssr", "G", 'M', "G??M"),
                new TestData("sr", "GYC", 'M', "GM")
        );
        for (TestData td : testData) {
            MatcherAssert.assertThat(FijiUtils.getDefaultColorSpecAsString(td.chanSpec, td.signalColors, td.referenceColor), equalTo(td.expectedColorSpec));
        }
    }

    @Test
    public void fijiColors() {
        class TestData {
            final String colorSpec;
            final String chanSpec;
            final List<FijiColor> expectedColors;

            public TestData(String colorSpec, String chanSpec, List<FijiColor> expectedColors) {
                this.colorSpec = colorSpec;
                this.chanSpec = chanSpec;
                this.expectedColors = expectedColors;
            }
        }
        List<TestData> testData = ImmutableList.of(
                new TestData("G,M", "sr", ImmutableList.of(new FijiColor('G', 1), new FijiColor('M', 2))),
                new TestData("#ff0000,#00ff00,#0000ff", "ssr", ImmutableList.of(new FijiColor('R', 1), new FijiColor('G', 1), new FijiColor('B', 1)))
        );
        for (TestData td : testData) {
            MatcherAssert.assertThat(FijiUtils.getColorSpec(td.colorSpec, td.chanSpec), equalTo(td.expectedColors));
        }

    }
}

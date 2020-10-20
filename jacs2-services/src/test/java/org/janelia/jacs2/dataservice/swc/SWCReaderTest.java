package org.janelia.jacs2.dataservice.swc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.common.collect.Iterables;

import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;

public class SWCReaderTest {

    private SWCReader swcReader;

    @Before
    public void setUp() {
        swcReader = new SWCReader();
    }

    @Test
    public void readSWC() {
        class TestData {
            final String swcFileName;
            final int headerListSize;
            final int nodesCount;
            final double[] offset;

            TestData(String swcFileName, int headerListSize, int nodesCount, double[] offset) {
                this.swcFileName = swcFileName;
                this.headerListSize = headerListSize;
                this.nodesCount = nodesCount;
                this.offset = offset;
            }
        }
        TestData[] testData  = new TestData[] {
                new TestData("src/test/resources/testdata/swc/test_dendrite.swc", 5, 450, new double[] {2, 3, 1}),
                new TestData("src/test/resources/testdata/swc/G-165.swc", 4, 660, new double[] {74923.550261987941, 18965.638017018195, 36545.230477000012})
        };
        for (TestData td : testData) {
            SWCData swcData = swcReader.readSWCStream(td.swcFileName, getTestInputStream(td.swcFileName));
            assertNotNull(swcData);
            MatcherAssert.assertThat(Iterables.size(swcData.getHeaderList()), equalTo(td.headerListSize));
            MatcherAssert.assertThat(Iterables.size(swcData.getNodeList()), equalTo(td.nodesCount));
            MatcherAssert.assertThat(swcData.extractOffset(), equalTo(td.offset));
        }
    }

    private InputStream getTestInputStream(String testFile) {
        try {
            return Files.newInputStream(Paths.get(testFile));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}

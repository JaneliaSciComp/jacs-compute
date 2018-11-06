package org.janelia.jacs2.dataservice.swc;

import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class SWCReaderTest {

    private static final String TEST_SWCFILE = "src/test/resources/testdata/swc/test_dendrite.swc";

    private SWCReader swcReader;

    @Before
    public void setUp() {
        swcReader = new SWCReader();
    }

    @Test
    public void readSWC() {
        SWCData swcData = swcReader.readSWCFile(Paths.get(TEST_SWCFILE));
        assertNotNull(swcData);
        assertThat(Iterables.size(swcData.getHeaderList()), equalTo(5));
        assertThat(Iterables.size(swcData.getNodeList()), equalTo(450));
        assertThat(swcData.extractOffset(), equalTo(new double[] {2, 3, 1}));
    }

}

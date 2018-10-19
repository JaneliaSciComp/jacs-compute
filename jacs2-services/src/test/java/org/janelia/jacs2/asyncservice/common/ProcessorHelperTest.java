package org.janelia.jacs2.asyncservice.common;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ProcessorHelperTest {

    private Map<String, String> jobResources = new HashMap<>();

    @Test
    public void calculateRequiredSlots() {
        class TestData {
            final String configuredCpuType;
            final String configuredMem;
            final String configuredSlots;
            final int expectedSlots;

            TestData(String configuredCpuType, String configuredMem, String configuredSlots, int expectedSlots) {
                this.configuredCpuType = configuredCpuType;
                this.configuredMem = configuredMem;
                this.configuredSlots = configuredSlots;
                this.expectedSlots = expectedSlots;
            }
        }
        TestData[] testData = new TestData[] {
                new TestData("broadwell", "96", "1", 7),
                new TestData("broadwell", "96", "8", 8),
                new TestData(null, "96", "1", 7),
                new TestData("haswell", "96", "16", 16)
        };
        for (TestData tdEntry : testData) {
            jobResources.put("cpuType", tdEntry.configuredCpuType);
            jobResources.put("memInGB", tdEntry.configuredMem);
            jobResources.put("nSlots", tdEntry.configuredSlots);
            assertEquals(tdEntry.expectedSlots, ProcessorHelper.getProcessingSlots(jobResources));
            jobResources.clear();
        }
    }
}

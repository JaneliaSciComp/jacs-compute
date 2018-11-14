package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class ExternalLSFDrmaaJobRunnerTest {

    private ExternalLSFDrmaaJobRunner lsfDrmaaJobRunner;

    @Before
    public void setUp() {
        lsfDrmaaJobRunner = new ExternalLSFDrmaaJobRunner(
                null, // drmaaSession
                null, // jacsServiceDataPersistence
                null, // accounting
                true,
                null // logger
        );
    }

    @Test
    public void nativeSpec() {
        class TestData {
            private final Map<String, String> jobResources;
            private final String billingAccount;
            private final String expectedResult;

            public TestData(Map<String, String> jobResources, String billingAccount, String expectedResult) {
                this.jobResources = jobResources;
                this.billingAccount = billingAccount;
                this.expectedResult = expectedResult;
            }
        }
        List<TestData> testData = ImmutableList.of(
                new TestData(ImmutableMap.of(), "", "-P \"\" "),
                new TestData(ImmutableMap.of(
                        "nSlots", "4"
                ), "", "-P \"\" -n 4 -R \"affinity[core(1)]\" "),
                new TestData(ImmutableMap.of(
                        "nSlots", "5",
                        "cpuType", "haswell"
                ), "billing", "-P \"billing\" -n 5 -R \"affinity[core(1)] select[haswell]\" "),
                new TestData(ImmutableMap.of(
                        "cpuType", "haswell",
                        "gridResourceLimits", "ssd_scratch,avx2"
                ), "billing", "-P \"billing\" -R \"select[haswell,ssd_scratch,avx2]\" "),
                new TestData(ImmutableMap.of(
                        "nSlots", "4",
                        "cpuType", "haswell",
                        "gridResourceLimits", "ssd_scratch,avx2"
                ), "", "-P \"\" -n 4 -R \"affinity[core(1)] select[haswell,ssd_scratch,avx2]\" ")
        );
        for (TestData td : testData) {
            assertThat(lsfDrmaaJobRunner.createNativeSpec(td.jobResources, td.billingAccount, ""), equalTo(td.expectedResult));
        }
    }
}

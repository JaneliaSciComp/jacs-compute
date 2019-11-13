package org.janelia.jacs2.asyncservice.lvtservices;

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Splitter;

import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.containerizedservices.PullAndRunSingularityContainerProcessor;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class KTXCreatorTest {
    private static final String TEST_WORKING_DIR = "testDir";
    private static final String KTX_CONTAINER_IMAGE = "testKTXContainer";

    private KTXCreator ktxCreator;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);
        ServiceComputationFactory serviceComputationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        PullAndRunSingularityContainerProcessor pullAndRunSingularityContainerProcessor = mock(PullAndRunSingularityContainerProcessor.class);

        ktxCreator = new KTXCreator(serviceComputationFactory, jacsServiceDataPersistence, TEST_WORKING_DIR, pullAndRunSingularityContainerProcessor, KTX_CONTAINER_IMAGE, logger);
    }

    @Test
    public void getAppBatchArgs() {
        Long testServiceId = 10L;
        int subTreeLength = 5;
        JacsServiceData testServiceData = createTestService(testServiceId,
                Arrays.asList(
                        "-inputDir", TEST_WORKING_DIR + "/octree",
                        "-outputDir", TEST_WORKING_DIR + "/ktx",
                        "-levels", "8",
                        "-subtreeLengthForSubjobSplitting", String.valueOf(subTreeLength)));
        KTXCreator.KTXCreatorArgs ktxArgs = ktxCreator.getArgs(testServiceData);
        String batchArgs = ktxCreator.getAppBatchArgs(ktxArgs);
        assertThat(Splitter.on(',').splitToList(batchArgs), hasSize((int) Math.pow(8, subTreeLength) + 1));
    }

    private JacsServiceData createTestService(Long serviceId, List<String> args) {
        JacsServiceData testServiceData = new JacsServiceDataBuilder(null)
                .addArgs(args)
                .build();
        testServiceData.setId(serviceId);
        return testServiceData;
    }

}

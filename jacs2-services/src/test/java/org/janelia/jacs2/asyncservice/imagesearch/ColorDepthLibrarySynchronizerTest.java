package org.janelia.jacs2.asyncservice.imagesearch;

import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsNotificationDao;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        ColorDepthLibrarySynchronizer.class,
        FileUtils.class
})
public class ColorDepthLibrarySynchronizerTest {

    private static final String TEST_WORKING_DIR = "testWorkingDir";
    private static final String TEST_CDMLIB_ROOT_DIR = "cdmlibRootDir";

    private ColorDepthLibrarySynchronizer colorDepthLibrarySynchronizer;

    @Before
    public void setUp() {
        Logger logger = mock(Logger.class);
        ServiceComputationFactory serviceComputationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        LegacyDomainDao legacyDao = mock(LegacyDomainDao.class);
        JacsNotificationDao jacsNotificationDao = mock(JacsNotificationDao.class);

        colorDepthLibrarySynchronizer = new ColorDepthLibrarySynchronizer(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                TEST_CDMLIB_ROOT_DIR,
                legacyDao,
                jacsNotificationDao,
                logger);
    }

    @Test
    public void cleanupRenamedSampleOnSync() {
        JacsServiceData testService = createTestServiceData(1L);
        PowerMockito.mockStatic(FileUtils.class);
    }

    private JacsServiceData createTestServiceData(Number serviceId) {
        JacsServiceData testServiceData = new JacsServiceDataBuilder(null)
                .addArgs("-alignmentSpace", "testalignment")
                .addArgs("-library", "testlib")
                .setName("colorDepthLibrarySync")
                .build();
        testServiceData.setId(serviceId);
        return testServiceData;
    }
}

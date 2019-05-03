package org.janelia.jacs2.asyncservice.imageservices;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.cdi.ObjectMapperFactory;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.JacsServiceState;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

public class DeconvolutionProcessorTest {
    private static final String DECONV_SCRIPT = "scripts/matlab_deconv";
    private static final String DEFAULT_WORKING_DIR = "testWorking";
    private static final String DEFAULT_EXECUTABLES_DIR = "testExecutables";
    private static final String MATLAB_ROOT_DIR = "testmatlab";
    private static final String MATLAB_LIB_DIRS = "bin/glnxa64,runtime/glnxa64,sys/os/glnxa64,sys/java/jre/glnxa64/jre/lib/amd64/native_threads";
    private static final String MATLAB_X11LIB_DIR = "X11/app-defaults";
    private static final String TEST_DATA_DIR = "src/test/resources/testdata/deconvolution";
    private static final String TEST_OWNER = "user:test";
    private static final Number TEST_SERVICE_ID = 1L;

    private DeconvolutionProcessor deconvolutionProcessor;

    @Before
    public void setUp() {
        JacsServiceDataPersistence jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        JacsJobInstanceInfoDao jacsJobInstanceInfoDao = mock(JacsJobInstanceInfoDao.class);

        Logger logger = mock(Logger.class);
        ServiceComputationFactory computationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);

        ApplicationConfig applicationConfig = new ApplicationConfigProvider().fromMap(
                ImmutableMap.of("Executables.ModuleBase", DEFAULT_EXECUTABLES_DIR))
                .build();

        Mockito.when(jacsServiceDataPersistence.findById(any(Number.class))).then(invocation -> {
            JacsServiceData sd = new JacsServiceData();
            sd.setId(invocation.getArgument(0));
            sd.setState(JacsServiceState.SUCCESSFUL);
            return sd;
        });
        Mockito.when(jacsServiceDataPersistence.createServiceIfNotFound(any(JacsServiceData.class))).then(invocation -> {
            JacsServiceData jacsServiceData = invocation.getArgument(0);
            jacsServiceData.setId(TEST_SERVICE_ID);
            jacsServiceData.setState(JacsServiceState.SUCCESSFUL); // mark the service as completed otherwise the computation doesn't return
            return jacsServiceData;
        });

        deconvolutionProcessor = new DeconvolutionProcessor(computationFactory,
                jacsServiceDataPersistence,
                null,
                DEFAULT_WORKING_DIR,
                jacsJobInstanceInfoDao,
                applicationConfig,
                ObjectMapperFactory.instance().newObjectMapper(),
                MATLAB_ROOT_DIR,
                MATLAB_LIB_DIRS,
                MATLAB_X11LIB_DIR,
                DECONV_SCRIPT,
                logger);
    }

    @Test
    public void prepareTaskConfigs() {
        JacsServiceData testService = createTestServiceData();
        List<ExternalCodeBlock> taskConfigs = deconvolutionProcessor.prepareConfigurationFiles(testService);
        assertTrue(taskConfigs.size() > 0);
    }

    private JacsServiceData createTestServiceData() {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .setOwnerKey(TEST_OWNER)
                .addArgs("-i", Paths.get(TEST_DATA_DIR, "488nm.json").toString())
                .addArgs("-i", Paths.get(TEST_DATA_DIR, "560nm.json").toString())
                .addArgs("-p", Paths.get(TEST_DATA_DIR, "pf1").toString())
                .addArgs("-p", Paths.get(TEST_DATA_DIR, "pf2").toString())
                .addArgs("-n", "10")
                .addArgs("-z", "0.1")
                .addArgs("-c", "8")
                ;
        JacsServiceData testServiceData = testServiceDataBuilder
                .build();
        testServiceData.setId(TEST_SERVICE_ID);
        testServiceData.setName("deconvolution");
        return testServiceData;
    }

}

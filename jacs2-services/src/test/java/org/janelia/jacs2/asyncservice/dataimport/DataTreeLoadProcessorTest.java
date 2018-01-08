package org.janelia.jacs2.asyncservice.dataimport;

import org.janelia.jacs2.asyncservice.common.ComputationTestUtils;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.imageservices.ImageMagickConverterProcessor;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dMipCmdProcessor;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.jacs2.dataservice.workspace.FolderService;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        SageLoaderProcessor.class,
        SageLoaderProcessor.SageLoaderErrorChecker.class
})
public class DataTreeLoadProcessorTest {
    private static final String DEFAULT_WORKING_DIR = "testWorking";

    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private ServiceComputationFactory computationFactory;
    private FolderService folderService;
    private StorageService storageService;
    private Vaa3dMipCmdProcessor vaa3dMipCmdProcessor;
    private ImageMagickConverterProcessor imageMagickConverterProcessor;
    private Logger logger;

    @Before
    public void setUp() {
        jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        logger = mock(Logger.class);
        computationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);
        folderService = mock(FolderService.class);
        storageService = mock(StorageService.class);
        vaa3dMipCmdProcessor = mock(Vaa3dMipCmdProcessor.class);
        imageMagickConverterProcessor = mock(ImageMagickConverterProcessor.class);
    }

    private DataTreeLoadProcessor createDataTreeLoadProcessor() {
        return new DataTreeLoadProcessor(computationFactory,
                jacsServiceDataPersistence,
                DEFAULT_WORKING_DIR,
                folderService,
                storageService,
                vaa3dMipCmdProcessor,
                imageMagickConverterProcessor,
                logger);
    }

    @Test
    public void processGifsAndPngs() {
        Long serviceId = 1L;
        String testFolder = "testLocation";
        String testLocation = "http://testStorage";
        JacsServiceData testService = createTestServiceData(serviceId, testFolder, testLocation);

        DataTreeLoadProcessor dataTreeLoadProcessor = createDataTreeLoadProcessor();

        dataTreeLoadProcessor.process(testService);
    }

    private JacsServiceData createTestServiceData(Number serviceId, String folderName, String storageLocation) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .setOwner("testOwner")
                .addArg("-extensionsToLoad", "img,v3draw,png")
                .addArg("-storageLocation", storageLocation)
                .addArg("-folderName", folderName)
                ;
        JacsServiceData testServiceData = testServiceDataBuilder.build();
        testServiceData.setId(serviceId);
        return testServiceData;
    }

}

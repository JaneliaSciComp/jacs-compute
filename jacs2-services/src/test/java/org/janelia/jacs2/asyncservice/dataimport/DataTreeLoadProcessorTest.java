package org.janelia.jacs2.asyncservice.dataimport;

import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.common.ComputationTestUtils;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.ImageMagickConverterProcessor;
import org.janelia.jacs2.asyncservice.imageservices.Vaa3dMipCmdProcessor;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.jacs2.dataservice.workspace.FolderService;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.workspace.TreeNode;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.JacsServiceState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        DataTreeLoadProcessor.class
})
public class DataTreeLoadProcessorTest {
    private static final String DEFAULT_WORKING_DIR = "testWorking";
    private static final String TEST_LOCAL_WORKSPACE = "testlocal";
    private static final Long TEST_DATA_NODE_ID = 10L;
    private static final Number TEST_SERVICE_ID = 1L;

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

        Mockito.when(vaa3dMipCmdProcessor.getMetadata()).thenCallRealMethod();
        Mockito.when(vaa3dMipCmdProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        )).thenCallRealMethod();

        Mockito.when(imageMagickConverterProcessor.getMetadata()).thenCallRealMethod();
        Mockito.when(imageMagickConverterProcessor.createServiceData(any(ServiceExecutionContext.class),
                any(ServiceArg.class),
                any(ServiceArg.class)
        )).thenCallRealMethod();

        Mockito.when(folderService.createFolder(any(Number.class), anyString(), anyString()))
                .then(invocation -> {
                    TreeNode dataFolder = new TreeNode();
                    dataFolder.setId(TEST_DATA_NODE_ID);
                    dataFolder.setName(invocation.getArgument(1));
                    dataFolder.setOwnerKey(invocation.getArgument(2));
                    return dataFolder;
                });
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
        String testOwner = "testOwner";
        String testFolder = "testLocation";
        String testLocation = "http://testStorage";
        JacsServiceData testService = createTestServiceData(serviceId, testOwner, testFolder, testLocation);

        DataTreeLoadProcessor dataTreeLoadProcessor = createDataTreeLoadProcessor();

        String testStorageRoot = "/storageRoot";
        String testStoragePrefix = "/storageRootPrefix";
        Mockito.when(storageService.listStorageContent(testLocation, testOwner))
                .thenReturn(ImmutableList.of(
                        new StorageService.StorageInfo(testLocation, testStorageRoot, testStoragePrefix,"", true),
                        new StorageService.StorageInfo(testLocation, testStorageRoot, testStoragePrefix,"f1.gif", false),
                        new StorageService.StorageInfo(testLocation, testStorageRoot, testStoragePrefix,"f2.png", false)
                ));

        ServiceComputation<JacsServiceResult<List<DataTreeLoadProcessor.DataLoadResult>>> dataLoadComputation = dataTreeLoadProcessor.process(testService);
        Consumer successful = mock(Consumer.class);
        Consumer failure = mock(Consumer.class);
        dataLoadComputation
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(folderService).createFolder(any(Number.class), eq(testFolder), eq(testOwner));
                    Mockito.verify(folderService).addImageFile(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                            eq(testStoragePrefix + "/f1.gif"),
                            eq(FileType.Unclassified2d),
                            eq(testOwner));
                    Mockito.verify(folderService).addImageFile(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                            eq(testStoragePrefix + "/f2.png"),
                            eq(FileType.SignalMip),
                            eq(testOwner));
                    Mockito.verifyNoMoreInteractions(folderService, vaa3dMipCmdProcessor, imageMagickConverterProcessor);
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });
    }

    @Test
    public void processLsmsAndVaa3ds() throws Exception {
        Long serviceId = 1L;
        String testOwner = "testOwner";
        String testFolder = "testLocation";
        String testLocation = "http://testStorage";
        JacsServiceData testService = createTestServiceData(serviceId, testOwner, testFolder, testLocation);

        DataTreeLoadProcessor dataTreeLoadProcessor = createDataTreeLoadProcessor();

        String testStorageRoot = "/storageRoot";
        String testStoragePrefix = "/storageRootPrefix";

        Mockito.when(storageService.listStorageContent(testLocation, testOwner))
                .thenReturn(ImmutableList.of(
                        new StorageService.StorageInfo(testLocation, testStorageRoot, testStoragePrefix,"", true),
                        new StorageService.StorageInfo(testLocation, testStorageRoot, testStoragePrefix,"f1.lsm", false),
                        new StorageService.StorageInfo(testLocation, testStorageRoot, testStoragePrefix,"f2.v3draw", false)
                ));

        Mockito.when(storageService.putStorageContent(anyString(), anyString(), anyString(), any(InputStream.class)))
                .then(invocation -> new StorageService.StorageInfo(testLocation, testStorageRoot, testStoragePrefix, invocation.getArgument(1), false));
        PowerMockito.mockStatic(Files.class);
        Mockito.when(Files.notExists(any(Path.class))).thenReturn(false);
        Mockito.when(Files.size(any(Path.class))).thenReturn(100L);

        Path f1TifPath = Paths.get(TEST_LOCAL_WORKSPACE + "/" + serviceId + "/temp/f1_mipArtifact.tif");
        Path f2TifPath = Paths.get(TEST_LOCAL_WORKSPACE + "/" + serviceId + "/temp/f2_mipArtifact.tif");
        File f1TifMipArtifact = mock(File.class);
        File f2TifMipArtifact = mock(File.class);
        Mockito.when(f1TifMipArtifact.getAbsolutePath()).thenReturn(f1TifPath.toString());
        Mockito.when(f1TifMipArtifact.toPath()).thenReturn(f1TifPath);
        Mockito.when(f2TifMipArtifact.getAbsolutePath()).thenReturn(f2TifPath.toString());
        Mockito.when(f2TifMipArtifact.toPath()).thenReturn(f2TifPath);

        ServiceResultHandler<List<File>> vaa3dMipCmdResultsResultHandler = mock(ServiceResultHandler.class);
        when(vaa3dMipCmdProcessor.getResultHandler()).thenReturn(vaa3dMipCmdResultsResultHandler);
        when(vaa3dMipCmdResultsResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(ImmutableList.of(f1TifMipArtifact, f2TifMipArtifact));

        Path f1PngPath = Paths.get(TEST_LOCAL_WORKSPACE + "/" + serviceId + "/temp/f1_mipArtifact.png");
        Path f2PngPath = Paths.get(TEST_LOCAL_WORKSPACE + "/" + serviceId + "/temp/f2_mipArtifact.png");
        File f1PngMipArtifact = mock(File.class);
        File f2PngMipArtifact = mock(File.class);
        FileInputStream f1PngMipStream = mock(FileInputStream.class);
        FileInputStream f2PngMipStream = mock(FileInputStream.class);
        Mockito.when(f1PngMipArtifact.toPath()).thenReturn(f1PngPath);
        Mockito.when(f2PngMipArtifact.toPath()).thenReturn(f2PngPath);
        PowerMockito.whenNew(FileInputStream.class).withArguments(f1PngPath.toFile()).thenReturn(f1PngMipStream);
        PowerMockito.whenNew(FileInputStream.class).withArguments(f2PngPath.toFile()).thenReturn(f2PngMipStream);

        ServiceResultHandler<List<File>> converterResultsResultHandler = mock(ServiceResultHandler.class);
        when(imageMagickConverterProcessor.getResultHandler()).thenReturn(converterResultsResultHandler);
        when(converterResultsResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(ImmutableList.of(f1PngMipArtifact, f2PngMipArtifact));

        ServiceComputation<JacsServiceResult<List<DataTreeLoadProcessor.DataLoadResult>>> dataLoadComputation = dataTreeLoadProcessor.process(testService);
        Consumer successful = mock(Consumer.class);
        Consumer failure = mock(Consumer.class);
        dataLoadComputation
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(folderService).createFolder(any(Number.class), eq(testFolder), eq(testOwner));
                    Mockito.verify(folderService).addImageFile(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                            eq(testStoragePrefix + "/f1.lsm"),
                            eq(FileType.LosslessStack),
                            eq(testOwner));
                    Mockito.verify(folderService).addImageFile(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                            eq(testStoragePrefix + "/f2.v3draw"),
                            eq(FileType.LosslessStack),
                            eq(testOwner));

                    Mockito.verify(vaa3dMipCmdProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-inputFiles", "testlocal/1/temp/f1.lsm,testlocal/1/temp/f2.v3draw"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputFiles", "testlocal/1/temp/f1_mipArtifact.tif,testlocal/1/temp/f2_mipArtifact.tif")))
                    );
                    Mockito.verify(imageMagickConverterProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ServiceArgMatcher(new ServiceArg("-inputFiles", "testlocal/1/temp/f1_mipArtifact.tif,testlocal/1/temp/f2_mipArtifact.tif"))),
                            argThat(new ServiceArgMatcher(new ServiceArg("-outputFiles", "testlocal/1/temp/f1_mipArtifact.png,testlocal/1/temp/f2_mipArtifact.png")))
                    );
                    Mockito.verify(folderService).addImageFile(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                            eq(testStoragePrefix + "/f1_mipArtifact.png"),
                            eq(FileType.SignalMip),
                            eq(testOwner));
                    Mockito.verify(folderService).addImageFile(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                            eq(testStoragePrefix + "/f2_mipArtifact.png"),
                            eq(FileType.SignalMip),
                            eq(testOwner));

                    Mockito.verifyNoMoreInteractions(folderService);
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });
    }

    private JacsServiceData createTestServiceData(Number serviceId, String owner, String folderName, String storageLocation) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .setOwnerKey(owner)
                .addArgs("-parentFolderId", String.valueOf(TEST_DATA_NODE_ID-1))
                .addArgs("-losslessImgExtensions", ".v3draw,.lsm")
                .addArgs("-storageLocation", storageLocation)
                .addArgs("-folderName", folderName)
                ;
        JacsServiceData testServiceData = testServiceDataBuilder
                .setWorkspace("testlocal")
                .build();
        testServiceData.setId(serviceId);
        testServiceData.setName("dataTreeLoad");
        return testServiceData;
    }

}

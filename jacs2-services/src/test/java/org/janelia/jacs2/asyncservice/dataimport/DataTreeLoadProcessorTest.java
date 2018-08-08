package org.janelia.jacs2.asyncservice.dataimport;

import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ResourceHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgMatcher;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceProcessorTestHelper;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.MIPsAndMoviesResult;
import org.janelia.jacs2.asyncservice.imageservices.MultiInputMIPsAndMoviesProcessor;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.jacs2.dataservice.workspace.FolderService;
import org.janelia.jacs2.testhelpers.ListArgMatcher;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.sample.Image;
import org.janelia.model.domain.workspace.TreeNode;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.JacsServiceState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        DataTreeLoadProcessor.class,
        StorageContentHelper.class
})
public class DataTreeLoadProcessorTest {
    private static final String DEFAULT_WORKING_DIR = "testWorking";
    private static final String TEST_LOCAL_WORKSPACE = "testDataTreeLocal";
    private static final Long TEST_DATA_NODE_ID = 10L;
    private static final Number TEST_SERVICE_ID = 1L;

    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private ServiceComputationFactory computationFactory;
    private FolderService folderService;
    private StorageService storageService;
    private MultiInputMIPsAndMoviesProcessor mipsConverterProcessor;
    private Logger logger;

    @Before
    public void setUp() {
        jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        logger = mock(Logger.class);
        computationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);
        folderService = mock(FolderService.class);
        storageService = mock(StorageService.class);
        mipsConverterProcessor = mock(MultiInputMIPsAndMoviesProcessor.class);

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

        ServiceProcessorTestHelper.prepareServiceProcessorMetadataAsRealCall(mipsConverterProcessor);

        Mockito.when(folderService.getOrCreateFolder(any(Number.class), anyString(), anyString()))
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
                mipsConverterProcessor,
                storageService,
                folderService,
                logger);
    }

    @Test
    public void processGifsAndPngs() {
        Long serviceId = 1L;
        String testStorageId = "testStorageId";
        String testOwner = "testOwner";
        String testFolder = "testLocation";
        String testLocation = "http://testStorage";
        String testAuthToken = "testAuthToken";
        JacsServiceData testService = createTestServiceData(serviceId, testOwner, testFolder, testLocation, testAuthToken, false, false);

        DataTreeLoadProcessor dataTreeLoadProcessor = createDataTreeLoadProcessor();

        String testStorageRoot = "/storageRoot";
        String testStoragePrefix = "/storageRootPrefix";
        Mockito.when(storageService.listStorageContent(testLocation, null, testOwner, testAuthToken))
                .thenReturn(ImmutableList.of(
                        new StorageService.StorageEntryInfo(testStorageId, testLocation, testLocation, testStorageRoot, testStoragePrefix,"", true),
                        new StorageService.StorageEntryInfo(testStorageId, testLocation, testLocation + "/" + "f1.gif", testStorageRoot, testStoragePrefix,"f1.gif", false),
                        new StorageService.StorageEntryInfo(testStorageId, testLocation, testLocation + "/" + "f2.png", testStorageRoot, testStoragePrefix,"f2.png", false)
                ));

        @SuppressWarnings("unchecked")
        ServiceResultHandler<List<MIPsAndMoviesResult>> mipsConverterResultHandler = mock(ServiceResultHandler.class);
        when(mipsConverterProcessor.getResultHandler()).thenReturn(mipsConverterResultHandler);
        when(mipsConverterResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(ImmutableList.of());

        Path basePath = Paths.get(TEST_LOCAL_WORKSPACE + "/" + serviceId + "/mips");

        ServiceComputation<JacsServiceResult<List<ContentStack>>> dataLoadComputation = dataTreeLoadProcessor.process(testService);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<List<ContentStack>>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        dataLoadComputation
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(mipsConverterProcessor).getMetadata();
                    Mockito.verify(mipsConverterProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ListArgMatcher<>(
                                    ImmutableList.of(
                                            new ServiceArgMatcher(new ServiceArg("-inputFiles", "")),
                                            new ServiceArgMatcher(new ServiceArg("-outputDir", basePath.toString())),
                                            new ServiceArgMatcher(new ServiceArg("-chanSpec", "r")),
                                            new ServiceArgMatcher(new ServiceArg("-colorSpec", "")),
                                            new ServiceArgMatcher(new ServiceArg("-options", "mips:movies"))
                                    )
                            ))
                    );
                    Mockito.verify(mipsConverterProcessor).getResultHandler();

                    Mockito.verify(folderService).getOrCreateFolder(any(Number.class), eq(testFolder), eq(testOwner));
                    Mockito.verify(folderService).addImageStack(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                            argThat(argument -> argument.getFilepath().equals(testStoragePrefix) &&
                                    argument.getFiles().entrySet().stream()
                                            .anyMatch(e -> e.getKey() == FileType.Unclassified2d && e.getValue().equals("f1.gif"))),
                            eq(testOwner));
                    Mockito.verify(folderService).addImageStack(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                            argThat(argument -> argument.getFilepath().equals(testStoragePrefix) &&
                                    argument.getFiles().entrySet().stream()
                                            .anyMatch(e -> e.getKey() == FileType.Unclassified2d && e.getValue().equals("f2.png"))),
                            eq(testOwner));
                    Mockito.verifyNoMoreInteractions(folderService, mipsConverterProcessor);
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
        String testStorageId = "testStorageId";
        String testOwner = "testOwner";
        String testFolder = "testLocation";
        String testLocation = "http://testStorage";
        String testAuthToken = "testAuthToken";
        JacsServiceData testService = createTestServiceData(serviceId, testOwner, testFolder, testLocation, testAuthToken, false, false);

        DataTreeLoadProcessor dataTreeLoadProcessor = createDataTreeLoadProcessor();

        String testStorageRoot = "/storageRoot";
        String testStoragePrefix = "/storageRootPrefix";

        Mockito.when(storageService.listStorageContent(testLocation, null, testOwner, testAuthToken))
                .thenReturn(ImmutableList.of(
                        new StorageService.StorageEntryInfo(testStorageId, testLocation, testLocation, testStorageRoot, testStoragePrefix,"", true),
                        new StorageService.StorageEntryInfo(testStorageId, testLocation, testLocation + "/" + "f1.lsm", testStorageRoot, testStoragePrefix,"f1.lsm", false),
                        new StorageService.StorageEntryInfo(testStorageId, testLocation, testLocation + "/" + "f2.v3draw", testStorageRoot, testStoragePrefix,"f2.v3draw", false)
                ));
        Mockito.when(storageService.getStorageContent(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(new ByteArrayInputStream("test".getBytes()));
        Mockito.when(storageService.putStorageContent(anyString(), anyString(), anyString(), anyString(), any(InputStream.class)))
                .then(invocation -> new StorageService.StorageEntryInfo(
                        testStorageId,
                        testLocation,
                        testLocation + "/entry_content/" + invocation.getArgument(1),
                        testStorageRoot,
                        testStoragePrefix,
                        invocation.getArgument(1),
                        false));

        PowerMockito.mockStatic(Files.class);
        Mockito.when(Files.createDirectories(any(Path.class))).then((Answer<Path>) invocation -> invocation.getArgument(0));
        Mockito.when(Files.copy(any(InputStream.class), any(Path.class), any(CopyOption.class))).then((Answer<Long>) invocation -> {
            InputStream is = invocation.getArgument(0);
            return (long) is.available();
        });

        Mockito.when(Files.notExists(any(Path.class))).thenReturn(true);
        Mockito.when(Files.exists(any(Path.class))).thenReturn(true);
        Mockito.when(Files.size(any(Path.class))).thenReturn(100L);

        Path basePath = Paths.get(TEST_LOCAL_WORKSPACE + "/" + serviceId);

        Path f1Path = basePath.resolve("mips/f1.lsm");
        Path f2Path = basePath.resolve("mips/f2.v3draw");

        Path f1PngPath = basePath.resolve("mips/f1_signal.png");
        Path f2PngPath = basePath.resolve("mips/f2_signal.png");

        @SuppressWarnings("unchecked")
        ServiceResultHandler<List<MIPsAndMoviesResult>> mipsConverterResultHandler = mock(ServiceResultHandler.class);
        when(mipsConverterProcessor.getResultHandler()).thenReturn(mipsConverterResultHandler);
        when(mipsConverterResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(ImmutableList.of(
                new MIPsAndMoviesResult(f1Path.toString()).addFile(f1PngPath.toString()),
                new MIPsAndMoviesResult(f2Path.toString()).addFile(f2PngPath.toString())
        ));
        File f1PngMipArtifact = mock(File.class);
        File f2PngMipArtifact = mock(File.class);
        FileInputStream f1PngMipStream = mock(FileInputStream.class);
        FileInputStream f2PngMipStream = mock(FileInputStream.class);
        Mockito.when(f1PngMipArtifact.toPath()).thenReturn(f1PngPath);
        Mockito.when(f2PngMipArtifact.toPath()).thenReturn(f2PngPath);
        PowerMockito.whenNew(FileInputStream.class).withArguments(f1PngPath.toFile()).thenReturn(f1PngMipStream);
        PowerMockito.whenNew(FileInputStream.class).withArguments(f2PngPath.toFile()).thenReturn(f2PngMipStream);

        ServiceComputation<JacsServiceResult<List<ContentStack>>> dataLoadComputation = dataTreeLoadProcessor.process(testService);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<List<ContentStack>>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        dataLoadComputation.thenApply(r -> {
            successful.accept(r);
            Mockito.verify(storageService).listStorageContent(
                    eq(testLocation),
                    isNull(),
                    eq(testOwner),
                    eq(testAuthToken)
            );
            Mockito.verify(storageService).getStorageContent(
                    eq(testLocation + "/" + f1Path.getFileName()),
                    eq(f1Path.getFileName().toString()),
                    eq(testOwner),
                    eq(testAuthToken)
            );
            Mockito.verify(storageService).getStorageContent(
                    eq(testLocation + "/" + f2Path.getFileName()),
                    eq(f2Path.getFileName().toString()),
                    eq(testOwner),
                    eq(testAuthToken)
            );

            Mockito.verify(mipsConverterProcessor).getMetadata();
            Mockito.verify(mipsConverterProcessor).createServiceData(any(ServiceExecutionContext.class),
                    argThat(new ListArgMatcher<>(
                            ImmutableList.of(
                                    new ServiceArgMatcher(new ServiceArg("-inputFiles", f1Path + "," + f2Path)),
                                    new ServiceArgMatcher(new ServiceArg("-outputDir", basePath.resolve("mips").toString())),
                                    new ServiceArgMatcher(new ServiceArg("-chanSpec", "r")),
                                    new ServiceArgMatcher(new ServiceArg("-colorSpec", "")),
                                    new ServiceArgMatcher(new ServiceArg("-options", "mips:movies"))
                            )
                    ))
            );
            Mockito.verify(mipsConverterProcessor).getResultHandler();

            Mockito.verify(storageService).putStorageContent(
                    eq(testLocation),
                    eq(basePath.relativize(f1PngPath).toString()),
                    eq(testOwner),
                    eq(testAuthToken),
                    any(InputStream.class)
            );
            Mockito.verify(storageService).putStorageContent(
                    eq(testLocation),
                    eq(basePath.relativize(f2PngPath).toString()),
                    eq(testOwner),
                    eq(testAuthToken),
                    any(InputStream.class)
            );
            Mockito.verify(folderService).getOrCreateFolder(any(Number.class), eq(testFolder), eq(testOwner));
            Mockito.verify(folderService).addImageStack(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                    argThat((Image argument) -> Stream.<Predicate<Image>>of(
                            (Image ti) -> ti.getFilepath().equals(testStoragePrefix),
                            (Image ti) -> ti.getFiles().entrySet().stream().anyMatch(e -> e.getKey() == FileType.SignalMip && e.getValue().equals("mips/f1_signal.png")),
                            (Image ti) -> ti.getFiles().entrySet().stream().anyMatch(e -> e.getKey() == FileType.LosslessStack && e.getValue().equals("f1.lsm"))
                            )
                            .reduce(Predicate::and)
                            .orElseGet(() -> t1 -> false)
                            .test(argument)
                    ),
                    eq(testOwner));
            Mockito.verify(folderService).addImageStack(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                    argThat((Image argument) -> Stream.<Predicate<Image>>of(
                            (Image ti) -> ti.getFilepath().equals(testStoragePrefix),
                            (Image ti) -> ti.getFiles().entrySet().stream().anyMatch(e -> e.getKey() == FileType.SignalMip && e.getValue().equals("mips/f2_signal.png")),
                            (Image ti) -> ti.getFiles().entrySet().stream().anyMatch(e -> e.getKey() == FileType.LosslessStack && e.getValue().equals("f2.v3draw"))
                            )
                            .reduce(Predicate::and)
                            .orElseGet(() -> t1 -> false)
                            .test(argument)
                    ),
                    eq(testOwner));
            Mockito.verifyNoMoreInteractions(mipsConverterProcessor, storageService, folderService);
            return r;
        });
        dataLoadComputation.exceptionally(exc -> {
            failure.accept(exc);
            fail(exc.toString());
            return null;
        });
    }

    @Test
    public void processLsmsAndVaa3dsWithoutMIPS() throws Exception {
        Long serviceId = 1L;
        String testStorageId = null;
        String testOwner = "testOwner";
        String testFolder = "testLocation";
        String testLocation = "http://testStorage";
        String testAuthToken = "testAuthToken";
        JacsServiceData testService = createTestServiceData(serviceId, testOwner, testFolder, testLocation, testAuthToken, true, false);

        DataTreeLoadProcessor dataTreeLoadProcessor = createDataTreeLoadProcessor();

        String testStorageRoot = "/storageRoot";
        String testStoragePrefix = "/storageRootPrefix";

        Mockito.when(storageService.listStorageContent(testLocation, null, testOwner, testAuthToken))
                .thenReturn(ImmutableList.of(
                        new StorageService.StorageEntryInfo(testStorageId, testLocation, testLocation, testStorageRoot, testStoragePrefix,"", true),
                        new StorageService.StorageEntryInfo(testStorageId, testLocation, testLocation + "/" + "f1.lsm", testStorageRoot, testStoragePrefix,"f1.lsm", false),
                        new StorageService.StorageEntryInfo(testStorageId, testLocation, testLocation + "/" + "f2.v3draw", testStorageRoot, testStoragePrefix,"f2.v3draw", false)
                ));
        Mockito.when(storageService.getStorageContent(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(new ByteArrayInputStream("test".getBytes()));
        Mockito.when(storageService.putStorageContent(anyString(), anyString(), anyString(), anyString(), any(InputStream.class)))
                .then(invocation -> new StorageService.StorageEntryInfo(
                        testStorageId,
                        testLocation,
                        testLocation + "/entry_content/" + invocation.getArgument(1),
                        testStorageRoot,
                        testStoragePrefix,
                        invocation.getArgument(1),
                        false));

        PowerMockito.mockStatic(Files.class);
        Mockito.when(Files.createDirectories(any(Path.class))).then((Answer<Path>) invocation -> invocation.getArgument(0));
        Mockito.when(Files.copy(any(InputStream.class), any(Path.class), any(CopyOption.class))).then((Answer<Long>) invocation -> {
            InputStream is = invocation.getArgument(0);
            return (long) is.available();
        });

        Mockito.when(Files.notExists(any(Path.class))).thenReturn(true);
        Mockito.when(Files.exists(any(Path.class))).thenReturn(true);
        Mockito.when(Files.size(any(Path.class))).thenReturn(100L);

        Path basePath = Paths.get(TEST_LOCAL_WORKSPACE + "/" + serviceId + "/mips");

        Path f1Path = basePath.resolve("f1.lsm");
        Path f2Path = basePath.resolve("f2.v3draw");

        Path f1PngPath = basePath.resolve("f1_signal.png");
        Path f2PngPath = basePath.resolve("f2_signal.png");

        @SuppressWarnings("unchecked")
        ServiceResultHandler<List<MIPsAndMoviesResult>> mipsConverterResultHandler = mock(ServiceResultHandler.class);
        when(mipsConverterProcessor.getResultHandler()).thenReturn(mipsConverterResultHandler);
        when(mipsConverterResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(ImmutableList.of(
                new MIPsAndMoviesResult(f1Path.toString()).addFile(f1PngPath.toString()),
                new MIPsAndMoviesResult(f2Path.toString()).addFile(f2PngPath.toString())
        ));

        File f1PngMipArtifact = mock(File.class);
        File f2PngMipArtifact = mock(File.class);
        FileInputStream f1PngMipStream = mock(FileInputStream.class);
        FileInputStream f2PngMipStream = mock(FileInputStream.class);
        Mockito.when(f1PngMipArtifact.toPath()).thenReturn(f1PngPath);
        Mockito.when(f2PngMipArtifact.toPath()).thenReturn(f2PngPath);
        PowerMockito.whenNew(FileInputStream.class).withArguments(f1PngPath.toFile()).thenReturn(f1PngMipStream);
        PowerMockito.whenNew(FileInputStream.class).withArguments(f2PngPath.toFile()).thenReturn(f2PngMipStream);

        ServiceComputation<JacsServiceResult<List<ContentStack>>> dataLoadComputation = dataTreeLoadProcessor.process(testService);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<List<ContentStack>>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        dataLoadComputation
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(storageService).listStorageContent(
                            eq(testLocation),
                            isNull(),
                            eq(testOwner),
                            eq(testAuthToken)
                    );

                    Mockito.verify(mipsConverterProcessor, never()).getMetadata();
                    Mockito.verify(mipsConverterProcessor, never()).createServiceData(any(ServiceExecutionContext.class),
                            any(ServiceArg.class),
                            any(ServiceArg.class)
                    );
                    Mockito.verify(mipsConverterProcessor, never()).getResultHandler();

                    Mockito.verify(folderService).getOrCreateFolder(any(Number.class), eq(testFolder), eq(testOwner));
                    Mockito.verify(folderService).addImageStack(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                            argThat((Image argument) -> Stream.<Predicate<Image>>of(
                                    (Image ti) -> ti.getFilepath().equals(testStoragePrefix),
                                    (Image ti) -> ti.getFiles().entrySet().stream().noneMatch(e -> e.getKey() == FileType.SignalMip && e.getValue().contains("f1_signal.png")),
                                    (Image ti) -> ti.getFiles().entrySet().stream().anyMatch(e -> e.getKey() == FileType.LosslessStack && e.getValue().equals("f1.lsm"))
                                    )
                                    .reduce(Predicate::and)
                                    .orElseGet(() -> t1 -> false)
                                    .test(argument)
                            ),
                            eq(testOwner));
                    Mockito.verify(folderService).addImageStack(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                            argThat((Image argument) -> Stream.<Predicate<Image>>of(
                                    (Image ti) -> ti.getFilepath().equals(testStoragePrefix),
                                    (Image ti) -> ti.getFiles().entrySet().stream().noneMatch(e -> e.getKey() == FileType.SignalMip && e.getValue().contains("f2_signal.png")),
                                    (Image ti) -> ti.getFiles().entrySet().stream().anyMatch(e -> e.getKey() == FileType.LosslessStack && e.getValue().equals("f2.v3draw"))
                                    )
                                    .reduce(Predicate::and)
                                    .orElseGet(() -> t1 -> false)
                                    .test(argument)
                            ),
                            eq(testOwner));

                    Mockito.verifyNoMoreInteractions(mipsConverterProcessor, storageService, folderService);
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });
    }

    @Test
    public void processLsmsAndVaa3dsWithStandaloneMIPs() throws Exception {
        Long serviceId = 1L;
        String testStorageId = "testStorageId";
        String testOwner = "testOwner";
        String testFolder = "testLocation";
        String testLocation = "http://testStorage";
        String testAuthToken = "testAuthToken";
        JacsServiceData testService = createTestServiceData(serviceId, testOwner, testFolder, testLocation, testAuthToken, false, true);

        DataTreeLoadProcessor dataTreeLoadProcessor = createDataTreeLoadProcessor();

        String testStorageRoot = "/storageRoot";
        String testStoragePrefix = "/storageRootPrefix";

        Mockito.when(storageService.listStorageContent(testLocation, null, testOwner, testAuthToken))
                .thenReturn(ImmutableList.of(
                        new StorageService.StorageEntryInfo(testStorageId, testLocation, testLocation, testStorageRoot, testStoragePrefix,"", true),
                        new StorageService.StorageEntryInfo(testStorageId, testLocation, testLocation + "/" + "f1.lsm", testStorageRoot, testStoragePrefix,"f1.lsm", false),
                        new StorageService.StorageEntryInfo(testStorageId, testLocation, testLocation + "/" + "f2.v3draw", testStorageRoot, testStoragePrefix,"f2.v3draw", false)
                ));
        Mockito.when(storageService.getStorageContent(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(new ByteArrayInputStream("test".getBytes()));
        Mockito.when(storageService.putStorageContent(anyString(), anyString(), anyString(), anyString(), any(InputStream.class)))
                .then(invocation -> new StorageService.StorageEntryInfo(
                        testStorageId,
                        testLocation,
                        testLocation + "/entry_content/" + invocation.getArgument(1),
                        testStorageRoot,
                        testStoragePrefix,
                        invocation.getArgument(1),
                        false));

        PowerMockito.mockStatic(Files.class);
        Mockito.when(Files.createDirectories(any(Path.class))).then((Answer<Path>) invocation -> invocation.getArgument(0));
        Mockito.when(Files.copy(any(InputStream.class), any(Path.class), any(CopyOption.class))).then((Answer<Long>) invocation -> {
            InputStream is = invocation.getArgument(0);
            return (long) is.available();
        });

        Mockito.when(Files.notExists(any(Path.class))).thenReturn(true);
        Mockito.when(Files.exists(any(Path.class))).thenReturn(true);
        Mockito.when(Files.size(any(Path.class))).thenReturn(100L);

        Path basePath = Paths.get(TEST_LOCAL_WORKSPACE + "/" + serviceId);

        Path f1Path = basePath.resolve("mips/f1.lsm");
        Path f2Path = basePath.resolve("mips/f2.v3draw");

        Path f1PngPath = basePath.resolve("mips/f1_signal.png");
        Path f2PngPath = basePath.resolve("mips/f2_signal.png");

        @SuppressWarnings("unchecked")
        ServiceResultHandler<List<MIPsAndMoviesResult>> mipsConverterResultHandler = mock(ServiceResultHandler.class);
        when(mipsConverterProcessor.getResultHandler()).thenReturn(mipsConverterResultHandler);
        when(mipsConverterResultHandler.getServiceDataResult(any(JacsServiceData.class))).thenReturn(ImmutableList.of(
                new MIPsAndMoviesResult(f1Path.toString()).addFile(f1PngPath.toString()),
                new MIPsAndMoviesResult(f2Path.toString()).addFile(f2PngPath.toString())
        ));
        File f1PngMipArtifact = mock(File.class);
        File f2PngMipArtifact = mock(File.class);
        FileInputStream f1PngMipStream = mock(FileInputStream.class);
        FileInputStream f2PngMipStream = mock(FileInputStream.class);
        Mockito.when(f1PngMipArtifact.toPath()).thenReturn(f1PngPath);
        Mockito.when(f2PngMipArtifact.toPath()).thenReturn(f2PngPath);
        PowerMockito.whenNew(FileInputStream.class).withArguments(f1PngPath.toFile()).thenReturn(f1PngMipStream);
        PowerMockito.whenNew(FileInputStream.class).withArguments(f2PngPath.toFile()).thenReturn(f2PngMipStream);

        ServiceComputation<JacsServiceResult<List<ContentStack>>> dataLoadComputation = dataTreeLoadProcessor.process(testService);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<List<ContentStack>>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        dataLoadComputation
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(storageService).listStorageContent(
                            eq(testLocation),
                            isNull(),
                            eq(testOwner),
                            eq(testAuthToken)
                    );
                    Mockito.verify(storageService).getStorageContent(
                            eq(testLocation + "/" + f1Path.getFileName()),
                            eq(f1Path.getFileName().toString()),
                            eq(testOwner),
                            eq(testAuthToken)
                    );
                    Mockito.verify(storageService).getStorageContent(
                            eq(testLocation + "/" + f2Path.getFileName()),
                            eq(f2Path.getFileName().toString()),
                            eq(testOwner),
                            eq(testAuthToken)
                    );

                    Mockito.verify(mipsConverterProcessor).getMetadata();
                    Mockito.verify(mipsConverterProcessor).createServiceData(any(ServiceExecutionContext.class),
                            argThat(new ListArgMatcher<>(
                                    ImmutableList.of(
                                            new ServiceArgMatcher(new ServiceArg("-inputFiles", f1Path + "," + f2Path)),
                                            new ServiceArgMatcher(new ServiceArg("-outputDir", basePath.resolve("mips").toString())),
                                            new ServiceArgMatcher(new ServiceArg("-chanSpec", "r")),
                                            new ServiceArgMatcher(new ServiceArg("-colorSpec", "")),
                                            new ServiceArgMatcher(new ServiceArg("-options", "mips:movies"))
                                    )
                            ))
                    );
                    Mockito.verify(mipsConverterProcessor).getResultHandler();

                    Mockito.verify(storageService).putStorageContent(
                            eq(testLocation),
                            eq(basePath.relativize(f1PngPath).toString()),
                            eq(testOwner),
                            eq(testAuthToken),
                            any(InputStream.class)
                    );
                    Mockito.verify(storageService).putStorageContent(
                            eq(testLocation),
                            eq(basePath.relativize(f2PngPath).toString()),
                            eq(testOwner),
                            eq(testAuthToken),
                            any(InputStream.class)
                    );
                    Mockito.verify(folderService).getOrCreateFolder(any(Number.class), eq(testFolder), eq(testOwner));
                    Mockito.verify(folderService).addImageFile(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                            eq(testStoragePrefix + "/f1.lsm"),
                            eq(FileType.LosslessStack),
                            eq(true),
                            eq(testOwner));
                    Mockito.verify(folderService).addImageFile(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                            eq(testStoragePrefix + "/mips/f1_signal.png"),
                            eq(FileType.SignalMip),
                            eq(true),
                            eq(testOwner));
                    Mockito.verify(folderService).addImageFile(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                            eq(testStoragePrefix + "/f2.v3draw"),
                            eq(FileType.LosslessStack),
                            eq(true),
                            eq(testOwner));
                    Mockito.verify(folderService).addImageFile(argThat(argument -> TEST_DATA_NODE_ID.equals(argument.getId())),
                            eq(testStoragePrefix + "/mips/f2_signal.png"),
                            eq(FileType.SignalMip),
                            eq(true),
                            eq(testOwner));

                    Mockito.verifyNoMoreInteractions(mipsConverterProcessor, storageService, folderService);
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    fail(exc.toString());
                    return null;
                });
    }

    private JacsServiceData createTestServiceData(Number serviceId, String owner, String folderName, String storageLocation, String authToken, boolean skipsMIPS, boolean standaloneMIPS) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .setOwnerKey(owner)
                .addArgs("-parentDataNodeId", String.valueOf(TEST_DATA_NODE_ID-1))
                .addArgs("-storageLocation", storageLocation)
                .addArgs("-dataNodeName", folderName)
                ;
        if (skipsMIPS) {
            testServiceDataBuilder.addArgs("-skipMIPS");
        } else if (standaloneMIPS) {
            testServiceDataBuilder.addArgs("-standaloneMIPS");
        }
        JacsServiceData testServiceData = testServiceDataBuilder
                .setWorkspace(TEST_LOCAL_WORKSPACE)
                .build();
        testServiceData.setId(serviceId);
        testServiceData.setName("generateMIPsForStorageContent");
        ResourceHelper.setAuthToken(testServiceData.getResources(), authToken);
        return testServiceData;
    }

}

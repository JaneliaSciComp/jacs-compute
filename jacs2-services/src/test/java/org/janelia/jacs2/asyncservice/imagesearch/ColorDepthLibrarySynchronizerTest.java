package org.janelia.jacs2.asyncservice.imagesearch;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsNotificationDao;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.domain.gui.cdmip.ColorDepthImage;
import org.janelia.model.domain.gui.cdmip.ColorDepthLibrary;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;

public class ColorDepthLibrarySynchronizerTest {

    private static final String TEST_WORKING_DIR = "testWorkingDir";
    private static final Long TEST_SERVICE_ID = 1L;
    private static Path testDirectory;

    private ServiceComputationFactory serviceComputationFactory;
    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private JacsNotificationDao jacsNotificationDao;
    private LegacyDomainDao legacyDao;
    private Logger logger;

    @BeforeClass
    public static void createTestDirectory() throws IOException {
        testDirectory = Files.createTempDirectory("colorDepthLibrarySynchronizer");
    }

    @AfterClass
    public static void cleanupTestDirectory() throws IOException {
        FileUtils.deletePath(testDirectory);
    }

    @Before
    public void setUp() {
        logger = mock(Logger.class);
        serviceComputationFactory = ComputationTestHelper.createTestServiceComputationFactory(logger);
        jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        legacyDao = mock(LegacyDomainDao.class);
        jacsNotificationDao = mock(JacsNotificationDao.class);
    }

    @Test
    public void checkCleanupRenamedSampleOnSyncWhenAllMIPsExist() {
        String testContext = "checkCleanupRenamedSampleOnSyncWhenAllMIPsExist";
        String testAlignmentSpace = "testAlignment";
        String testLib = "testLib";

        JacsServiceData testService = createTestServiceData(testAlignmentSpace, testLib);

        Mockito.when(legacyDao.getDomainObjects(null, ColorDepthLibrary.class))
                .thenReturn(ImmutableList.of(createTestCDMIPLibrary(testLib)));

        List<String> testFiles = prepareColorDepthMIPsFiles(testContext, testAlignmentSpace, testLib,
                new String[]{
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1_CDM.png",
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2_CDM.png",
                        // the next 2 file correspond to the same sample ID but they have different sample name so only 1 set of these two will survive
                        "GMR_86D05_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1_CDM.png",
                        "GMR_86D05_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2_CDM.png"
                },
                new String[]{});

        Mockito.when(legacyDao.getColorDepthPaths(null, testLib, testAlignmentSpace)).thenReturn(testFiles);

        ColorDepthLibrarySynchronizer colorDepthLibrarySynchronizer = new ColorDepthLibrarySynchronizer(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                testDirectory.resolve(testContext).toString(),
                legacyDao,
                jacsNotificationDao,
                logger);

        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        colorDepthLibrarySynchronizer.process(testService)
                .thenApply(r -> {
                    successful.accept(r);
                    try {
                        Mockito.verify(legacyDao, Mockito.never()).save(anyString(), ArgumentMatchers.any(ColorDepthImage.class));
                    } catch (Exception e) {
                        Assert.fail(e.toString());
                    }
                    // 2 existing samples had the same sample id but different name so they should have been deleted
                    Mockito.verify(legacyDao, Mockito.times(2)).deleteDomainObject(isNull(), eq(ColorDepthImage.class), anyLong());
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    Assert.fail(exc.toString());
                    return null;
                });
    }

    @Test
    public void checkColorDepthMIPsCreatedWhenNewerRenamedSamplesAdded() {
        String testContext = "checkColorDepthMIPsCreatedWhenNewerRenamedSamplesAdded";
        String testAlignmentSpace = "testAlignment";
        String testLib = "testLib";
        JacsServiceData testService = createTestServiceData(testAlignmentSpace, testLib);

        Mockito.when(legacyDao.getDomainObjects(null, ColorDepthLibrary.class))
                .thenReturn(ImmutableList.of(createTestCDMIPLibrary(testLib)));

        List<String> testFiles = prepareColorDepthMIPsFiles(testContext, testAlignmentSpace, testLib,
                new String[]{
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1_CDM.png",
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2_CDM.png",
                        // the next 2 file correspond to the same sample ID but they have different sample name so only 1 set of these two will survive
                        "GMR_86D05_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1_CDM.png",
                        "GMR_86D05_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2_CDM.png"
                },
                new String[]{});

        Mockito.when(legacyDao.getColorDepthPaths(null, testLib, testAlignmentSpace))
                .thenReturn(testFiles.stream().filter(fn -> fn.contains("GMR_83B04")).collect(Collectors.toList()))
                .thenReturn(testFiles);

        ColorDepthLibrarySynchronizer colorDepthLibrarySynchronizer = new ColorDepthLibrarySynchronizer(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                testDirectory.resolve(testContext).toString(),
                legacyDao,
                jacsNotificationDao,
                logger);

        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        colorDepthLibrarySynchronizer.process(testService)
                .thenApply(r -> {
                    successful.accept(r);
                    try {
                        Mockito.verify(legacyDao, Mockito.times(2)).save(isNull(), ArgumentMatchers.any(ColorDepthImage.class));
                    } catch (Exception e) {
                        Assert.fail(e.toString());
                    }
                    // 2 existing samples had the same sample id but different name so they should have been deleted
                    Mockito.verify(legacyDao, Mockito.times(2)).deleteDomainObject(isNull(), eq(ColorDepthImage.class), anyLong());
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    Assert.fail(exc.toString());
                    return null;
                });
    }

    @Test
    public void checkColorDepthMIPsCreatedWhenOlderRenamedSampleFilesArePresentInCDLibDir() {
        String testContext = "checkColorDepthMIPsCreatedWhenOlderRenamedSampleFilesExist";
        String testAlignmentSpace = "testAlignment";
        String testLib = "testLib";
        JacsServiceData testService = createTestServiceData(testAlignmentSpace, testLib);

        Mockito.when(legacyDao.getDomainObjects(null, ColorDepthLibrary.class))
                .thenReturn(ImmutableList.of(createTestCDMIPLibrary(testLib)));

        List<String> testFiles = prepareColorDepthMIPsFiles(testContext, testAlignmentSpace, testLib,
                new String[]{
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1_CDM.png",
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2_CDM.png",
                        // the next 2 file correspond to the same sample ID but they have different sample name so only 1 set of these two will survive
                        "GMR_86D05_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1_CDM.png",
                        "GMR_86D05_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2_CDM.png"
                },
                new String[]{});

        Mockito.when(legacyDao.getColorDepthPaths(null, testLib, testAlignmentSpace))
                .thenReturn(testFiles.stream().filter(fn -> fn.contains("GMR_86D05")).collect(Collectors.toList())) // newer files exist in the mips
                ;

        ColorDepthLibrarySynchronizer colorDepthLibrarySynchronizer = new ColorDepthLibrarySynchronizer(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                testDirectory.resolve(testContext).toString(),
                legacyDao,
                jacsNotificationDao,
                logger);

        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        colorDepthLibrarySynchronizer.process(testService)
                .thenApply(r -> {
                    successful.accept(r);
                    try {
                        Mockito.verify(legacyDao, Mockito.never()).save(isNull(), ArgumentMatchers.any(ColorDepthImage.class));
                    } catch (Exception e) {
                        Assert.fail(e.toString());
                    }
                    // 2 existing samples had the same sample id but different name so they should have been deleted
                    Mockito.verify(legacyDao, Mockito.never()).deleteDomainObject(isNull(), eq(ColorDepthImage.class), anyLong());
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    Assert.fail(exc.toString());
                    return null;
                });
    }

    @Test
    public void createColorDepthLibraryVersion() {
        String testContext = "createColorDepthLibraryVersion";
        String testAlignmentSpace = "testAlignment";
        String testLib = "testLib";
        JacsServiceData testService = createTestServiceData(testAlignmentSpace, testLib);

        Mockito.when(legacyDao.getDomainObjects(null, ColorDepthLibrary.class))
                .thenReturn(ImmutableList.of(createTestCDMIPLibrary(testLib)));

        String[] testFileNames = new String[] {
                "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1_CDM.png",
                "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2_CDM.png"
        };
        String[] testVersionNames = new String[] {
                "ver1",
                "ver2"
        };

        List<String> testFiles = prepareColorDepthMIPsFiles(testContext, testAlignmentSpace, testLib,
                testFileNames,
                testVersionNames);

        Mockito.when(legacyDao.getColorDepthPaths(null, testLib, testAlignmentSpace))
                .thenReturn(testFiles)
        ;

        ColorDepthLibrarySynchronizer colorDepthLibrarySynchronizer = new ColorDepthLibrarySynchronizer(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                testDirectory.resolve(testContext).toString(),
                legacyDao,
                jacsNotificationDao,
                logger);

        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        colorDepthLibrarySynchronizer.process(testService)
                .thenApply(r -> {
                    successful.accept(r);
                    try {
                        Mockito.verify(legacyDao, Mockito.times(testVersionNames.length)).save(ArgumentMatchers.eq("group:flylight"), ArgumentMatchers.any(ColorDepthLibrary.class));
                        Mockito.verify(legacyDao, Mockito.times(testVersionNames.length * testFileNames.length)).save(ArgumentMatchers.eq("group:flylight"), ArgumentMatchers.any(ColorDepthImage.class));
                    } catch (Exception e) {
                        Assert.fail(e.toString());
                    }
                    // 2 existing samples had the same sample id but different name so they should have been deleted
                    Mockito.verify(legacyDao, Mockito.never()).deleteDomainObject(isNull(), eq(ColorDepthImage.class), anyLong());
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    Assert.fail(exc.toString());
                    return null;
                });
    }

    private ColorDepthLibrary createTestCDMIPLibrary(String lib) {
        ColorDepthLibrary cdLib = new ColorDepthLibrary();
        cdLib.setIdentifier(lib);
        cdLib.setName(lib);
        return cdLib;
    }

    private JacsServiceData createTestServiceData(String testAlignmentSpace, String testLib) {
        JacsServiceData testServiceData = new JacsServiceDataBuilder(null)
                .addArgs("-alignmentSpace", testAlignmentSpace)
                .addArgs("-library", testLib)
                .setName("colorDepthLibrarySync")
                .build();
        testServiceData.setId(TEST_SERVICE_ID);
        return testServiceData;
    }

    private List<String> prepareColorDepthMIPsFiles(String context, String testAlignmentSpace, String testLib, String[] testFilenames, String[] testLibVersions) {
        Path testLibDir = testDirectory.resolve(context).resolve(testAlignmentSpace).resolve(testLib);
        try {
            Files.createDirectories(testLibDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Stream.of(testLibVersions)
                .forEach(libVersion -> {
                    Path libVersionDir = testLibDir.resolve(libVersion);
                    try {
                        Files.createDirectories(libVersionDir);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    // for this test same file names are added to the version
                    createTestColorDepthImageFiles(libVersionDir, testFilenames);
                });

        return createTestColorDepthImageFiles(testLibDir, testFilenames);
    }

    private List<String> createTestColorDepthImageFiles(Path testLibDir, String[] filenames) {
        return Streams.zip(IntStream.range(0, filenames.length).boxed(), Stream.of(filenames), (i, fn) -> ImmutablePair.of(i, testLibDir.resolve(fn)))
                .map(indexedTestFilePath -> {
                    try {
                        Files.copy(new ByteArrayInputStream("test".getBytes()), indexedTestFilePath.getRight());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    File testCDFile = indexedTestFilePath.getRight().toFile();
                    testCDFile.setLastModified(System.currentTimeMillis() - (filenames.length - indexedTestFilePath.getLeft()) * 1000);
                    Mockito.when(legacyDao.getColorDepthImageByPath(null, testCDFile.getAbsolutePath())).then(invocation -> {
                        String cdfile = invocation.getArgument(1);
                        ColorDepthImage cdi = new ColorDepthImage();
                        cdi.setId(testCDFile.lastModified());
                        cdi.setFilepath(cdfile);
                        return cdi;
                    });
                    return testCDFile;
                })
                .map(File::getAbsolutePath)
                .collect(Collectors.toList());
    }
}

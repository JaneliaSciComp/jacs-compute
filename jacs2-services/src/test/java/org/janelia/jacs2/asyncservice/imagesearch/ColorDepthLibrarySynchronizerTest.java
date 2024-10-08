package org.janelia.jacs2.asyncservice.imagesearch;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.jacs2.asyncservice.common.ComputationTestHelper;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsNotificationDao;
import org.janelia.model.access.domain.dao.AnnotationDao;
import org.janelia.model.access.domain.dao.ColorDepthImageDao;
import org.janelia.model.access.domain.dao.ColorDepthImageQuery;
import org.janelia.model.access.domain.dao.ColorDepthLibraryDao;
import org.janelia.model.access.domain.dao.DatasetDao;
import org.janelia.model.access.domain.dao.EmBodyDao;
import org.janelia.model.access.domain.dao.EmDataSetDao;
import org.janelia.model.access.domain.dao.LineReleaseDao;
import org.janelia.model.access.domain.dao.SubjectDao;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.gui.cdmip.ColorDepthImage;
import org.janelia.model.domain.gui.cdmip.ColorDepthLibrary;
import org.janelia.model.domain.ontology.Annotation;
import org.janelia.model.domain.sample.LineRelease;
import org.janelia.model.domain.sample.Sample;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;

public class ColorDepthLibrarySynchronizerTest {

    private static final String TEST_WORKING_DIR = "testWorkingDir";
    private static final Long TEST_SERVICE_ID = 1L;
    public static final String TEST_OWNER_KEY = "group:test";
    private static Path testDirectory;

    private ServiceComputationFactory serviceComputationFactory;
    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private JacsNotificationDao jacsNotificationDao;
    private SubjectDao subjectDao;
    private ColorDepthImageDao colorDepthImageDao;
    private ColorDepthLibraryDao colorDepthLibraryDao;
    private LineReleaseDao lineReleaseDao;
    private AnnotationDao annotationDao;
    private EmDataSetDao emDataSetDao;
    private EmBodyDao emBodyDao;
    private DatasetDao datasetDao;
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
        subjectDao = mock(SubjectDao.class);
        colorDepthImageDao = mock(ColorDepthImageDao.class);
        colorDepthLibraryDao = mock(ColorDepthLibraryDao.class);
        lineReleaseDao = mock(LineReleaseDao.class);
        annotationDao = mock(AnnotationDao.class);
        emDataSetDao = mock(EmDataSetDao.class);
        emBodyDao = mock(EmBodyDao.class);
        datasetDao = mock(DatasetDao.class);
        jacsNotificationDao = mock(JacsNotificationDao.class);
        Random mipIdGen = new Random();
        Mockito.when(colorDepthImageDao.saveBySubjectKey(any(ColorDepthImage.class), ArgumentMatchers.argThat(argument -> true)))
                .then(invocation -> {
                    ColorDepthImage cdi = invocation.getArgument(0);
                    cdi.setId(mipIdGen.nextLong());
                    return cdi;
                });
    }

    @Test
    public void checkCleanupRenamedSampleOnSyncWhenAllMIPsExist() {
        String testContext = "checkCleanupRenamedSampleOnSyncWhenAllMIPsExist";
        String testAlignmentSpace = "JRC2018_Unisex_20x_HR";
        String testLib = "testLib";

        JacsServiceData testService = createFSSyncOnlyServiceData(testAlignmentSpace, testLib);

        Mockito.when(colorDepthLibraryDao.findAll(0, -1))
                .thenReturn(ImmutableList.of(createTestCDMIPLibrary(testLib)));

        Map<String, String[]> testFileNames = ImmutableMap.of(
                testLib, new String[]{
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1_CDM.png",
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2_CDM.png",
                        // the next 2 file correspond to the same sample ID but they have different sample name so only 1 set of these two will survive
                        "GMR_86D05_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1_CDM.png",
                        "GMR_86D05_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2_CDM.png"
                }
        );
        Map<String, List<String>> testFilesByLibraryIdentifier = prepareColorDepthMIPsFiles(testContext, testAlignmentSpace, testFileNames);

        testFilesByLibraryIdentifier.forEach((libId, libFiles) -> {
            Mockito.when(colorDepthImageDao.streamColorDepthMIPs(
                    new ColorDepthImageQuery()
                            .withLibraryIdentifiers(Collections.singleton(libId.replace('/', '_')))
                            .withAlignmentSpace(testAlignmentSpace)))
                    .then(invocation -> libFiles.stream()
                            .map(f -> {
                                ColorDepthImage mip = new ColorDepthImage();
                                mip.setFilepath(f);
                                return mip;
                            }));
        });

        ColorDepthLibrarySynchronizer colorDepthLibrarySynchronizer = new ColorDepthLibrarySynchronizer(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                testDirectory.resolve(testContext).toString(),
                subjectDao,
                colorDepthLibraryDao,
                colorDepthImageDao,
                lineReleaseDao,
                annotationDao,
                emDataSetDao,
                emBodyDao,
                datasetDao,
                jacsNotificationDao,
                TEST_OWNER_KEY,
                logger);

        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        colorDepthLibrarySynchronizer.process(testService)
                .thenApply(r -> {
                    successful.accept(r);
                    try {
                        Mockito.verify(colorDepthImageDao, Mockito.never()).saveBySubjectKey(any(ColorDepthImage.class), anyString());
                    } catch (Exception e) {
                        Assert.fail(e.toString());
                    }
                    // 2 existing samples had the same sample id but different name so they should have been deleted
                    Mockito.verify(colorDepthImageDao, Mockito.times(2)).delete(any(ColorDepthImage.class));
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
        String testAlignmentSpace = "JRC2018_Unisex_20x_HR";
        String testLib = "testLib";
        JacsServiceData testService = createFSSyncOnlyServiceData(testAlignmentSpace, testLib);

        Mockito.when(colorDepthLibraryDao.findAll(0, -1))
                .thenReturn(ImmutableList.of(createTestCDMIPLibrary(testLib)));

        Map<String, String[]> testFileNames = ImmutableMap.of(
                testLib, new String[]{
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1_CDM.png",
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2_CDM.png",
                        // the next 2 file correspond to the same sample ID but they have different sample name so only 1 set of these two will survive
                        "GMR_86D05_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1_CDM.png",
                        "GMR_86D05_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2_CDM.png"
                }
        );
        Map<String, List<String>> testFilesByLibraryIdentifier = prepareColorDepthMIPsFiles(testContext, testAlignmentSpace, testFileNames);

        testFilesByLibraryIdentifier.forEach((libId, libFiles) -> {
            Mockito.when(colorDepthImageDao.streamColorDepthMIPs(
                    new ColorDepthImageQuery()
                            .withLibraryIdentifiers(Collections.singleton(libId.replace('/', '_')))
                            .withAlignmentSpace(testAlignmentSpace)))
                    .thenReturn(libFiles.stream().filter(fn -> fn.contains("GMR_83B04")).map(f -> {
                        ColorDepthImage mip = new ColorDepthImage();
                        mip.setFilepath(f);
                        return mip;
                    }))
                    .thenReturn(libFiles.stream().map(f -> {
                        ColorDepthImage mip = new ColorDepthImage();
                        mip.setFilepath(f);
                        return mip;
                    }))
                    ;
        });

        ColorDepthLibrarySynchronizer colorDepthLibrarySynchronizer = new ColorDepthLibrarySynchronizer(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                testDirectory.resolve(testContext).toString(),
                subjectDao,
                colorDepthLibraryDao,
                colorDepthImageDao,
                lineReleaseDao,
                annotationDao,
                emDataSetDao,
                emBodyDao,
                datasetDao,
                jacsNotificationDao,
                TEST_OWNER_KEY,
                logger);

        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        colorDepthLibrarySynchronizer.process(testService)
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(colorDepthImageDao, Mockito.times(2)).saveBySubjectKey(any(ColorDepthImage.class), isNull());

                    // 2 existing samples had the same sample id but different name so they should have been deleted
                    Mockito.verify(colorDepthImageDao, Mockito.times(2)).delete(any(ColorDepthImage.class));
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
        JacsServiceData testService = createFSSyncOnlyServiceData(testAlignmentSpace, testLib);

        Mockito.when(colorDepthLibraryDao.findAll(0, -1))
                .thenReturn(ImmutableList.of(createTestCDMIPLibrary(testLib)));

        Map<String, String[]> testFileNames = ImmutableMap.of(
                testLib, new String[]{
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1_CDM.png",
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2_CDM.png",
                        // the next 2 file correspond to the same sample ID but they have different sample name so only 1 set of these two will survive
                        "GMR_86D05_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1_CDM.png",
                        "GMR_86D05_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2_CDM.png"
                }
        );
        Map<String, List<String>> testFilesByLibraryIdentifier = prepareColorDepthMIPsFiles(testContext, testAlignmentSpace, testFileNames);

        testFilesByLibraryIdentifier.forEach((libId, libFiles) -> {
            Mockito.when(colorDepthImageDao.streamColorDepthMIPs(
                    new ColorDepthImageQuery()
                            .withLibraryIdentifiers(Collections.singleton(libId.replace('/', '_')))
                            .withAlignmentSpace(testAlignmentSpace)))
                    .then(invocation -> libFiles.stream()
                            .filter(fn -> fn.contains("GMR_86D05"))
                            .map(f -> {
                                ColorDepthImage mip = new ColorDepthImage();
                                mip.setFilepath(f);
                                return mip;
                            }));
        });

        ColorDepthLibrarySynchronizer colorDepthLibrarySynchronizer = new ColorDepthLibrarySynchronizer(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                testDirectory.resolve(testContext).toString(),
                subjectDao,
                colorDepthLibraryDao,
                colorDepthImageDao,
                lineReleaseDao,
                annotationDao,
                emDataSetDao,
                emBodyDao,
                datasetDao,
                jacsNotificationDao,
                TEST_OWNER_KEY,
                logger);

        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        colorDepthLibrarySynchronizer.process(testService)
                .thenApply(r -> {
                    successful.accept(r);
                    try {
                        Mockito.verify(colorDepthImageDao, Mockito.never()).saveBySubjectKey(any(ColorDepthImage.class), isNull());
                    } catch (Exception e) {
                        Assert.fail(e.toString());
                    }
                    // 2 existing samples had the same sample id but different name so they should have been deleted
                    Mockito.verify(colorDepthImageDao, Mockito.never()).delete(any(ColorDepthImage.class));
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    Assert.fail(exc.toString());
                    return null;
                });
    }

    @Test
    public void createColorDepthLMLibraryVersion() {
        String testContext = "createColorDepthLMLibraryVersion";
        String testAlignmentSpace = "JRC2018_Unisex_20x_HR";
        String testLib = "testLMLib";
        JacsServiceData testService = createFSSyncOnlyServiceData(testAlignmentSpace, testLib);

        Mockito.when(colorDepthLibraryDao.findAll(0, -1))
                .thenReturn(ImmutableList.of(createTestCDMIPLibrary(testLib)));

        Map<String, String[]> testLibsWithFiles = ImmutableMap.of(
                testLib, new String[]{
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1_CDM.png",
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2_CDM.png"
                },
                testLib + "/ver1", new String[]{
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1-ver1_CDM.png",
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2-ver1_CDM.png"
                },
                testLib + "/ver1/ver1_1", new String[]{
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1-ver1-ver1_1_CDM.png",
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2-ver1-ver1_1_CDM.png"
                },
                testLib + "/ver2", new String[]{
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH1-ver2_CDM.png",
                        "GMR_83B04_AE_01-20190423_63_B1-40x-Brain-JRC2018_Unisex_20x_HR-2663179940551196770-CH2-ver2_CDM.png"
                }
        );
        Map<String, List<String>> testFilesByLibraryIdentifier = prepareColorDepthMIPsFiles(testContext, testAlignmentSpace, testLibsWithFiles);

        testFilesByLibraryIdentifier.forEach((libId, libFiles) -> {
            Mockito.when(colorDepthImageDao.streamColorDepthMIPs(
                    new ColorDepthImageQuery()
                            .withLibraryIdentifiers(Collections.singleton(libId.replace('/', '_')))
                            .withAlignmentSpace(testAlignmentSpace)))
                    .thenReturn(Stream.of())
                    .thenReturn(libFiles.stream().map(f -> {
                        ColorDepthImage mip = new ColorDepthImage();
                        mip.setFilepath(f);
                        return mip;
                    }));
        });

        Map<Long, ColorDepthLibrary> createdCDLibraries = new HashMap<>();

        try {
            Mockito.when(colorDepthLibraryDao.saveBySubjectKey(any(ColorDepthLibrary.class), ArgumentMatchers.argThat(argument -> true)))
                    .then(invocation -> {
                        ColorDepthLibrary cdl = invocation.getArgument(0);
                        cdl.setId(createdCDLibraries.size() + 1L);
                        createdCDLibraries.put(cdl.getId(), cdl);
                        return cdl;
                    });
            Mockito.when(colorDepthLibraryDao.findById(anyLong()))
                    .then(invocation -> createdCDLibraries.get(invocation.getArgument(0)));
        } catch (Exception e) {
            Assert.fail(e.toString());
        }

        ColorDepthLibrarySynchronizer colorDepthLibrarySynchronizer = new ColorDepthLibrarySynchronizer(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                testDirectory.resolve(testContext).toString(),
                subjectDao,
                colorDepthLibraryDao,
                colorDepthImageDao,
                lineReleaseDao,
                annotationDao,
                emDataSetDao,
                emBodyDao,
                datasetDao,
                jacsNotificationDao,
                TEST_OWNER_KEY,
                logger);

        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        colorDepthLibrarySynchronizer.process(testService)
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(colorDepthLibraryDao, Mockito.times(testLibsWithFiles.size()*2-1)).saveBySubjectKey(any(ColorDepthLibrary.class), ArgumentMatchers.argThat(arg -> true));
                    Mockito.verify(colorDepthImageDao, Mockito.times((int) testLibsWithFiles.entrySet().stream().flatMap(e -> Arrays.stream(e.getValue())).count())).saveBySubjectKey(any(ColorDepthImage.class), ArgumentMatchers.argThat(arg -> true));
                    // 2 existing samples had the same sample id but different name so they should have been deleted
                    Mockito.verify(colorDepthImageDao, Mockito.never()).delete(any(ColorDepthImage.class));
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    Assert.fail(exc.toString());
                    return null;
                });
    }

    @Test
    public void createColorDepthEMLibraryVersion() {
        String testContext = "createColorDepthEMLibraryVersion";
        String testAlignmentSpace = "JRC2018_Unisex_20x_HR";
        String testLib = "testEMLib";
        JacsServiceData testService = createFSSyncOnlyServiceData(testAlignmentSpace, testLib);

        Mockito.when(colorDepthLibraryDao.findAll(0, -1))
                .thenReturn(ImmutableList.of(createTestCDMIPLibrary(testLib)));

        // cover some naming use cases that we currently have for flyem libraries
        Map<String, String[]> testLibsWithFiles = ImmutableMap.of(
                testLib, new String[] {
                        "987877232-FB4K-RT_18U.tif",
                        "987273073--RT_18U.tif",
                        "985818067.tif"
                },
                testLib + "/fl", new String[] {
                        "987877232-FB4K-RT_18U_FL.tif",
                        "987273073--RT_18U_FL.tif",
                        "985818067_FL.tif"
                },
                testLib + "/cdm", new String[] {
                        "987877232-FB4K-RT_18U-01_CDM.tif",
                        "987877232-FB4K-RT_18U_FL-02_CDM.tif",
                        "987273073--RT_18U-01_CDM.tif",
                        "987273073--RT_18U_FL-02_CDM.tif",
                        "985818067-01_CDM.tif",
                        "985818067_FL-02_CDM.tif"
                },
                testLib + "/grad", new String[] {
                        "987877232-FB4K-RT_18U-01_CDM.tif",
                        "987877232-FB4K-RT_18U_FL-02_CDM.tif",
                        "987273073--RT_18U-01_CDM.tif",
                        "987273073--RT_18U_FL-02_CDM.tif",
                        "985818067-01_CDM.tif",
                        "985818067_FL-02_CDM.tif"
                },
                testLib + "/zgap", new String[] {
                        "987877232-FB4K-RT_18U-01_CDM.tif",
                        "987877232-FB4K-RT_18U_FL-02_CDM.tif",
                        "987273073--RT_18U-01_CDM.tif",
                        "987273073--RT_18U_FL-02_CDM.tif",
                        "985818067-01_CDM.tif",
                        "985818067_FL-02_CDM.tif"
                }
        );
        Map<String, List<String>> testFilesByLibraryIdentifier = prepareColorDepthMIPsFiles(testContext, testAlignmentSpace, testLibsWithFiles);

        testFilesByLibraryIdentifier.forEach((libId, libFiles) -> {
            Mockito.when(colorDepthImageDao.streamColorDepthMIPs(
                            new ColorDepthImageQuery()
                                    .withLibraryIdentifiers(Collections.singleton(libId.replace('/', '_')))
                                    .withAlignmentSpace(testAlignmentSpace)))
                    .thenReturn(Stream.of())
                    .thenReturn(libFiles.stream().map(f -> {
                        ColorDepthImage mip = new ColorDepthImage();
                        mip.setFilepath(f);
                        return mip;
                    }));
        });

        Map<Long, ColorDepthLibrary> createdCDLibraries = new HashMap<>();

        try {
            Mockito.when(colorDepthLibraryDao.saveBySubjectKey(any(ColorDepthLibrary.class), ArgumentMatchers.argThat(argument -> true)))
                    .then(invocation -> {
                        ColorDepthLibrary cdl = invocation.getArgument(0);
                        cdl.setId(createdCDLibraries.size() + 1L);
                        createdCDLibraries.put(cdl.getId(), cdl);
                        return cdl;
                    });
            Mockito.when(colorDepthLibraryDao.findById(anyLong()))
                    .then(invocation -> createdCDLibraries.get(invocation.getArgument(0)));
        } catch (Exception e) {
            Assert.fail(e.toString());
        }

        ColorDepthLibrarySynchronizer colorDepthLibrarySynchronizer = new ColorDepthLibrarySynchronizer(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                testDirectory.resolve(testContext).toString(),
                subjectDao,
                colorDepthLibraryDao,
                colorDepthImageDao,
                lineReleaseDao,
                annotationDao,
                emDataSetDao,
                emBodyDao,
                datasetDao,
                jacsNotificationDao,
                TEST_OWNER_KEY,
                logger);

        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        colorDepthLibrarySynchronizer.process(testService)
                .thenApply(r -> {
                    successful.accept(r);
                    Mockito.verify(colorDepthLibraryDao, Mockito.times(testLibsWithFiles.size()*2-1)).saveBySubjectKey(any(ColorDepthLibrary.class), ArgumentMatchers.argThat(arg -> true));
                    Mockito.verify(colorDepthImageDao, Mockito.times((int) testLibsWithFiles.entrySet().stream().flatMap(e -> Arrays.stream(e.getValue())).count())).saveBySubjectKey(any(ColorDepthImage.class), ArgumentMatchers.argThat(arg -> true));
                    // 2 existing samples had the same sample id but different name so they should have been deleted
                    Mockito.verify(colorDepthImageDao, Mockito.never()).delete(any(ColorDepthImage.class));
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

    private JacsServiceData createFSSyncOnlyServiceData(String testAlignmentSpace, String testLib) {
        JacsServiceData testServiceData = new JacsServiceDataBuilder(null)
                .addArgs("-alignmentSpace", testAlignmentSpace)
                .addArgs("-library", testLib)
                .setName("colorDepthLibrarySync")
                .build();
        testServiceData.setId(TEST_SERVICE_ID);
        return testServiceData;
    }

    private Map<String, List<String>> prepareColorDepthMIPsFiles(String context, String testAlignmentSpace, Map<String, String[]> testFilenames) {
        Path alignmentSpaceDir = testDirectory.resolve(context).resolve(testAlignmentSpace);
        try {
            Files.createDirectories(alignmentSpaceDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return testFilenames.entrySet().stream()
                .map(libFilesEntry -> {
                    Path testLibDir = alignmentSpaceDir.resolve(libFilesEntry.getKey());
                    try {
                        Files.createDirectories(testLibDir);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    return ImmutablePair.of(libFilesEntry.getKey(), createTestColorDepthImageFiles(testLibDir, libFilesEntry.getValue()));
                })
                .collect(Collectors.toMap(libFilesEntry -> libFilesEntry.getLeft(), libFilesEntry -> libFilesEntry.getRight()));
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
                    Mockito.when(colorDepthImageDao.streamColorDepthMIPs(any(ColorDepthImageQuery.class))).then(invocation -> {
                        ColorDepthImage cdi = new ColorDepthImage();
                        cdi.setId(testCDFile.lastModified());
                        cdi.setFilepath(testCDFile.getAbsolutePath());
                        return Stream.of(cdi);
                    });
                    Mockito.when(colorDepthImageDao.findColorDepthImageByPath(testCDFile.getAbsolutePath())).then(invocation -> {
                        String cdfile = invocation.getArgument(0);
                        ColorDepthImage cdi = new ColorDepthImage();
                        cdi.setId(testCDFile.lastModified());
                        cdi.setFilepath(cdfile);
                        return Optional.of(cdi);
                    });
                    return testCDFile;
                })
                .map(File::getAbsolutePath)
                .collect(Collectors.toList());
    }

    /**
     * Test for partitioning algorithm used for library discovery based on release
     */
    @Test
    public void simplePartitioning() {
        List<Reference> testData = LongStream.rangeClosed(1, 120)
                .mapToObj(i -> Reference.createFor(Sample.class, i))
                .collect(Collectors.toList());
        String testIdentifier = "testlib";
        AtomicInteger counter = new AtomicInteger();
        Map<Integer, Long> results = testData.stream()
                .collect(Collectors.groupingBy(
                        ref -> counter.getAndIncrement() / 25,
                        Collectors.collectingAndThen(
                                Collectors.toSet(),
                                sampleRefs -> colorDepthImageDao.addLibraryBySampleRefs(testIdentifier, "20x", sampleRefs, false))));
        Assert.assertEquals(120 / 25 + 1, results.size());
        Mockito.verify(colorDepthImageDao, Mockito.times(results.size())).addLibraryBySampleRefs(eq(testIdentifier), anyString(), anySet(), anyBoolean());
    }

    @Test
    public void discoverLibrariesByReleases() {
        JacsServiceData testService = createReleaseSyncOnlyServiceData();

        Mockito.when(lineReleaseDao.findAll(0, -1))
                .thenReturn(ImmutableList.of(
                        createLineRelease(1L, "site 1",
                                Arrays.asList(Reference.createFor("Sample#1"), Reference.createFor("Sample#2"), Reference.createFor("Sample#3"))),
                        createLineRelease(2L, "site 2",
                                Arrays.asList(Reference.createFor("Sample#4"), Reference.createFor("Sample#5"), Reference.createFor("Sample#6"))),
                        createLineRelease(3L, "site 1",
                                Arrays.asList(Reference.createFor("Sample#7"), Reference.createFor("Sample#8"), Reference.createFor("Sample#9")))
                ));
        Mockito.when(annotationDao.findAnnotationsByTargets(anySet()))
                .then(invocation -> {
                    Collection<Reference> refs = invocation.getArgument(0);
                    return refs.stream()
                            .map(sr -> {
                                Annotation a = new Annotation();
                                a.setName("Publish20xToWeb");
                                a.setTarget(sr);
                                return a;
                            })
                            .collect(Collectors.toList());
                });
        Mockito.when(colorDepthImageDao.addLibraryBySampleRefs(
                "flylight_site_1_published",
                "20x",
                ImmutableSet.of(
                        Reference.createFor("Sample#1"), Reference.createFor("Sample#2"), Reference.createFor("Sample#3"),
                        Reference.createFor("Sample#7"), Reference.createFor("Sample#8"), Reference.createFor("Sample#9")
                ),
                false))
                .then((Answer<Long>) invocation -> {
                    Collection<Reference> refs = invocation.getArgument(2);
                    return (long) refs.size();
                });
        Mockito.when(colorDepthImageDao.addLibraryBySampleRefs(
                "flylight_site_2_published",
                "20x",
                ImmutableSet.of(
                        Reference.createFor("Sample#4"), Reference.createFor("Sample#5"), Reference.createFor("Sample#6")
                ),
                false))
                .then((Answer<Long>) invocation -> {
                    Collection<Reference> refs = invocation.getArgument(2);
                    return (long) refs.size();
                });
        ColorDepthLibrarySynchronizer colorDepthLibrarySynchronizer = new ColorDepthLibrarySynchronizer(serviceComputationFactory,
                jacsServiceDataPersistence,
                TEST_WORKING_DIR,
                testDirectory.resolve("").toString(),
                subjectDao,
                colorDepthLibraryDao,
                colorDepthImageDao,
                lineReleaseDao,
                annotationDao,
                emDataSetDao,
                emBodyDao,
                datasetDao,
                jacsNotificationDao,
                TEST_OWNER_KEY,
                logger);
        @SuppressWarnings("unchecked")
        Consumer<JacsServiceResult<Void>> successful = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Throwable> failure = mock(Consumer.class);
        colorDepthLibrarySynchronizer.process(testService)
                .thenApply(r -> {
                    successful.accept(r);
                    try {
                        Mockito.verify(colorDepthLibraryDao, Mockito.times(2)).saveBySubjectKey(any(ColorDepthLibrary.class), ArgumentMatchers.argThat(arg -> true));
                        Mockito.verify(colorDepthImageDao).countColorDepthMIPsByAlignmentSpaceForLibrary("flylight_site_1_published");
                        Mockito.verify(colorDepthImageDao).countColorDepthMIPsByAlignmentSpaceForLibrary("flylight_site_2_published");
                    } catch (Exception e) {
                        Assert.fail(e.toString());
                    }
                    // 2 existing samples had the same sample id but different name so they should have been deleted
                    Mockito.verify(colorDepthImageDao, Mockito.never()).delete(any(ColorDepthImage.class));
                    return r;
                })
                .exceptionally(exc -> {
                    failure.accept(exc);
                    Assert.fail(exc.toString());
                    return null;
                });

    }

    private LineRelease createLineRelease(Long id, String site, List<Reference> sampleRefs) {
        LineRelease lr = new LineRelease();
        lr.setId(id);
        lr.setTargetWebsite(site);
        lr.addReaders(Collections.singleton("group:flylight"));
        sampleRefs.forEach(lr::addChild);
        return lr;
    }

    private JacsServiceData createReleaseSyncOnlyServiceData() {
        JacsServiceData testServiceData = new JacsServiceDataBuilder(null)
                .addArgs("-skipFileDiscovery")
                .addArgs("-includePublishedDiscovery")
                .setName("colorDepthLibrarySync")
                .build();
        testServiceData.setId(TEST_SERVICE_ID);
        return testServiceData;
    }

}

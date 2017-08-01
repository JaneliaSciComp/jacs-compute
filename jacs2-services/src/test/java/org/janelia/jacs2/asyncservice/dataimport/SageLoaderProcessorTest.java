package org.janelia.jacs2.asyncservice.dataimport;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.janelia.jacs2.asyncservice.common.ComputationTestUtils;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.cdi.ApplicationConfigProvider;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceDataBuilder;
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
import java.util.List;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({
        SageLoaderProcessor.class,
        SageLoaderProcessor.SageLoaderErrorChecker.class
})
public class SageLoaderProcessorTest {

    private JacsServiceDataPersistence jacsServiceDataPersistence;
    private ServiceComputationFactory computationFactory;
    private Logger logger;
    private String perlExecutable = "perl";
    private String libraryPath = "testLibrary";
    private String scriptName = "testScript";
    private String defaultWorkingDir = "testWorking";
    private String executablesBaseDir = "testTools";

    @Before
    public void setUp() {
        jacsServiceDataPersistence = mock(JacsServiceDataPersistence.class);
        logger = mock(Logger.class);
        computationFactory = ComputationTestUtils.createTestServiceComputationFactory(logger);
    }

    private SageLoaderProcessor createSageLoaderProcessor(String sageEnvironment) {
        ApplicationConfig applicationConfig = new ApplicationConfigProvider().fromMap(
                ImmutableMap.of(
                        "Executables.ModuleBase", executablesBaseDir,
                        "Sage.write.environment", sageEnvironment))
                .build();
        return new SageLoaderProcessor(computationFactory,
                jacsServiceDataPersistence,
                null, // serviceRunners are not essential for these unit tests
                defaultWorkingDir,
                perlExecutable,
                libraryPath,
                scriptName,
                ComputationTestUtils.createTestThrottledProcessesQueue(),
                applicationConfig,
                logger);

    }

    @Test
    public void notAllImagesFoundInProductionMode() throws Exception {
        List<String> testImages = ImmutableList.of("im1", "im2", "im3", "im4");
        Long serviceId = 1L;
        String serviceStdOut = "testOut";
        String serviceStdErr = "testErr";
        JacsServiceData testService = createTestServiceData(serviceId, testImages, serviceStdOut, serviceStdErr);

        File serviceOutFile = Mockito.mock(File.class);
        File serviceErrFile = Mockito.mock(File.class);
        PowerMockito.whenNew(File.class).withArguments(serviceStdOut).thenReturn(serviceOutFile);
        PowerMockito.whenNew(File.class).withArguments(serviceStdErr).thenReturn(serviceErrFile);
        PowerMockito.when(serviceOutFile.exists()).thenReturn(true);
        PowerMockito.when(serviceErrFile.exists()).thenReturn(true);

        FileInputStream serviceOutStream = Mockito.mock(FileInputStream.class);
        FileInputStream serviceErrStream = Mockito.mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withArguments(serviceStdOut).thenReturn(serviceOutStream);
        PowerMockito.whenNew(FileInputStream.class).withArguments(serviceStdErr).thenReturn(serviceErrStream);

        Mockito.when(serviceOutStream.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
                .then(invocation -> {
                    byte[] buffer = invocation.getArgument(0);
                    int offset = invocation.getArgument(1);
                    int length = invocation.getArgument(2);
                    byte[] srcBuffer = "Images found: 3\n".getBytes();
                    Preconditions.checkArgument(srcBuffer.length <= length);
                    System.arraycopy(srcBuffer, 0, buffer, offset, srcBuffer.length);
                    return srcBuffer.length;
                })
                .then(invocation -> {
                    byte[] buffer = invocation.getArgument(0);
                    int offset = invocation.getArgument(1);
                    int length = invocation.getArgument(2);
                    byte[] srcBuffer = "Images inserted: 1\n".getBytes();
                    Preconditions.checkArgument(srcBuffer.length <= length);
                    System.arraycopy(srcBuffer, 0, buffer, offset, srcBuffer.length);
                    return srcBuffer.length;
                })
                .thenReturn(0);

        Mockito.when(serviceErrStream.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
                .thenReturn(0);

        SageLoaderProcessor sageLoaderProcessor = createSageLoaderProcessor("production");

        List<String> errors =  sageLoaderProcessor.getErrorChecker().collectErrors(testService);
        assertThat(errors, hasItem("Not all images found - expected 4 but only found 3"));
    }

    @Test
    public void allImagesFoundInProductionMode() throws Exception {
        List<String> testImages = ImmutableList.of("im1", "im2", "im3", "im4");
        Long serviceId = 1L;
        String serviceStdOut = "testOut";
        String serviceStdErr = "testErr";
        JacsServiceData testService = createTestServiceData(serviceId, testImages, serviceStdOut, serviceStdErr);

        File serviceOutFile = Mockito.mock(File.class);
        File serviceErrFile = Mockito.mock(File.class);
        PowerMockito.whenNew(File.class).withArguments(serviceStdOut).thenReturn(serviceOutFile);
        PowerMockito.whenNew(File.class).withArguments(serviceStdErr).thenReturn(serviceErrFile);
        PowerMockito.when(serviceOutFile.exists()).thenReturn(true);
        PowerMockito.when(serviceErrFile.exists()).thenReturn(true);

        FileInputStream serviceOutStream = Mockito.mock(FileInputStream.class);
        FileInputStream serviceErrStream = Mockito.mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withArguments(serviceStdOut).thenReturn(serviceOutStream);
        PowerMockito.whenNew(FileInputStream.class).withArguments(serviceStdErr).thenReturn(serviceErrStream);

        Mockito.when(serviceOutStream.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
                .then(invocation -> {
                    byte[] buffer = invocation.getArgument(0);
                    int offset = invocation.getArgument(1);
                    int length = invocation.getArgument(2);
                    byte[] srcBuffer = "Images found: 4\n".getBytes();
                    Preconditions.checkArgument(srcBuffer.length <= length);
                    System.arraycopy(srcBuffer, 0, buffer, offset, srcBuffer.length);
                    return srcBuffer.length;
                })
                .then(invocation -> {
                    byte[] buffer = invocation.getArgument(0);
                    int offset = invocation.getArgument(1);
                    int length = invocation.getArgument(2);
                    byte[] srcBuffer = "Images inserted: 0\n".getBytes();
                    Preconditions.checkArgument(srcBuffer.length <= length);
                    System.arraycopy(srcBuffer, 0, buffer, offset, srcBuffer.length);
                    return srcBuffer.length;
                })
                .thenReturn(0);

        Mockito.when(serviceErrStream.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
                .thenReturn(0);

        SageLoaderProcessor sageLoaderProcessor = createSageLoaderProcessor("production");

        List<String> errors =  sageLoaderProcessor.getErrorChecker().collectErrors(testService);
        assertThat(errors, hasSize(0));
    }

    @Test
    public void notAllImagesFoundInDevMode() throws Exception {
        List<String> testImages = ImmutableList.of("im1", "im2", "im3", "im4", "im5");
        Long serviceId = 1L;
        String serviceStdOut = "testOut";
        String serviceStdErr = "testErr";
        JacsServiceData testService = createTestServiceData(serviceId, testImages, serviceStdOut, serviceStdErr);

        File serviceOutFile = Mockito.mock(File.class);
        File serviceErrFile = Mockito.mock(File.class);
        PowerMockito.whenNew(File.class).withArguments(serviceStdOut).thenReturn(serviceOutFile);
        PowerMockito.whenNew(File.class).withArguments(serviceStdErr).thenReturn(serviceErrFile);
        PowerMockito.when(serviceOutFile.exists()).thenReturn(true);
        PowerMockito.when(serviceErrFile.exists()).thenReturn(true);

        FileInputStream serviceOutStream = Mockito.mock(FileInputStream.class);
        FileInputStream serviceErrStream = Mockito.mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withArguments(serviceStdOut).thenReturn(serviceOutStream);
        PowerMockito.whenNew(FileInputStream.class).withArguments(serviceStdErr).thenReturn(serviceErrStream);

        Mockito.when(serviceOutStream.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
                .then(invocation -> {
                    byte[] buffer = invocation.getArgument(0);
                    int offset = invocation.getArgument(1);
                    int length = invocation.getArgument(2);
                    byte[] srcBuffer = "Images found: 3\n".getBytes();
                    Preconditions.checkArgument(srcBuffer.length <= length);
                    System.arraycopy(srcBuffer, 0, buffer, offset, srcBuffer.length);
                    return srcBuffer.length;
                })
                .then(invocation -> {
                    byte[] buffer = invocation.getArgument(0);
                    int offset = invocation.getArgument(1);
                    int length = invocation.getArgument(2);
                    byte[] srcBuffer = "Images inserted: 1\n".getBytes();
                    Preconditions.checkArgument(srcBuffer.length <= length);
                    System.arraycopy(srcBuffer, 0, buffer, offset, srcBuffer.length);
                    return srcBuffer.length;
                })
                .thenReturn(0);

        Mockito.when(serviceErrStream.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
                .thenReturn(0);

        SageLoaderProcessor sageLoaderProcessor = createSageLoaderProcessor("dev");

        List<String> errors =  sageLoaderProcessor.getErrorChecker().collectErrors(testService);
        assertThat(errors, hasItem("Not all images found - expected 5 but only found 3 and inserted 1"));
    }

    @Test
    public void allImagesFoundInDevMode() throws Exception {
        List<String> testImages = ImmutableList.of("im1", "im2", "im3", "im4", "im5");
        Long serviceId = 1L;
        String serviceStdOut = "testOut";
        String serviceStdErr = "testErr";
        JacsServiceData testService = createTestServiceData(serviceId, testImages, serviceStdOut, serviceStdErr);

        File serviceOutFile = Mockito.mock(File.class);
        File serviceErrFile = Mockito.mock(File.class);
        PowerMockito.whenNew(File.class).withArguments(serviceStdOut).thenReturn(serviceOutFile);
        PowerMockito.whenNew(File.class).withArguments(serviceStdErr).thenReturn(serviceErrFile);
        PowerMockito.when(serviceOutFile.exists()).thenReturn(true);
        PowerMockito.when(serviceErrFile.exists()).thenReturn(true);

        FileInputStream serviceOutStream = Mockito.mock(FileInputStream.class);
        FileInputStream serviceErrStream = Mockito.mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withArguments(serviceStdOut).thenReturn(serviceOutStream);
        PowerMockito.whenNew(FileInputStream.class).withArguments(serviceStdErr).thenReturn(serviceErrStream);

        Mockito.when(serviceOutStream.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
                .then(invocation -> {
                    byte[] buffer = invocation.getArgument(0);
                    int offset = invocation.getArgument(1);
                    int length = invocation.getArgument(2);
                    byte[] srcBuffer = "Images found: 5\n".getBytes();
                    Preconditions.checkArgument(srcBuffer.length <= length);
                    System.arraycopy(srcBuffer, 0, buffer, offset, srcBuffer.length);
                    return srcBuffer.length;
                })
                .then(invocation -> {
                    byte[] buffer = invocation.getArgument(0);
                    int offset = invocation.getArgument(1);
                    int length = invocation.getArgument(2);
                    byte[] srcBuffer = "Images inserted: 0\n".getBytes();
                    Preconditions.checkArgument(srcBuffer.length <= length);
                    System.arraycopy(srcBuffer, 0, buffer, offset, srcBuffer.length);
                    return srcBuffer.length;
                })
                .thenReturn(0);

        Mockito.when(serviceErrStream.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
                .thenReturn(0);

        SageLoaderProcessor sageLoaderProcessor = createSageLoaderProcessor("dev");

        List<String> errors =  sageLoaderProcessor.getErrorChecker().collectErrors(testService);
        assertThat(errors, hasSize(0));
    }

    @Test
    public void imagesFoundPlusImagesInsertedMatchInDevMode() throws Exception {
        List<String> testImages = ImmutableList.of("im1", "im2", "im3", "im4", "im5");
        Long serviceId = 1L;
        String serviceStdOut = "testOut";
        String serviceStdErr = "testErr";
        JacsServiceData testService = createTestServiceData(serviceId, testImages, serviceStdOut, serviceStdErr);

        File serviceOutFile = Mockito.mock(File.class);
        File serviceErrFile = Mockito.mock(File.class);
        PowerMockito.whenNew(File.class).withArguments(serviceStdOut).thenReturn(serviceOutFile);
        PowerMockito.whenNew(File.class).withArguments(serviceStdErr).thenReturn(serviceErrFile);
        PowerMockito.when(serviceOutFile.exists()).thenReturn(true);
        PowerMockito.when(serviceErrFile.exists()).thenReturn(true);

        FileInputStream serviceOutStream = Mockito.mock(FileInputStream.class);
        FileInputStream serviceErrStream = Mockito.mock(FileInputStream.class);
        PowerMockito.whenNew(FileInputStream.class).withArguments(serviceStdOut).thenReturn(serviceOutStream);
        PowerMockito.whenNew(FileInputStream.class).withArguments(serviceStdErr).thenReturn(serviceErrStream);

        Mockito.when(serviceOutStream.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
                .then(invocation -> {
                    byte[] buffer = invocation.getArgument(0);
                    int offset = invocation.getArgument(1);
                    int length = invocation.getArgument(2);
                    byte[] srcBuffer = "Images found: 5\n".getBytes();
                    Preconditions.checkArgument(srcBuffer.length <= length);
                    System.arraycopy(srcBuffer, 0, buffer, offset, srcBuffer.length);
                    return srcBuffer.length;
                })
                .then(invocation -> {
                    byte[] buffer = invocation.getArgument(0);
                    int offset = invocation.getArgument(1);
                    int length = invocation.getArgument(2);
                    byte[] srcBuffer = "Images inserted: 0\n".getBytes();
                    Preconditions.checkArgument(srcBuffer.length <= length);
                    System.arraycopy(srcBuffer, 0, buffer, offset, srcBuffer.length);
                    return srcBuffer.length;
                })
                .thenReturn(0);

        Mockito.when(serviceErrStream.read(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
                .thenReturn(0);

        SageLoaderProcessor sageLoaderProcessor = createSageLoaderProcessor("dev");

        List<String> errors =  sageLoaderProcessor.getErrorChecker().collectErrors(testService);
        assertThat(errors, hasSize(0));
    }

    private JacsServiceData createTestServiceData(Number serviceId, List<String> sampleFiles, String serviceStdOut, String serviceStdErr) {
        JacsServiceDataBuilder testServiceDataBuilder = new JacsServiceDataBuilder(null)
                .setOwner("testOwner")
                .addArg("-configFile", "config.txt")
                .addArg("-grammarFile", "grammar.txt")
                .addArg("-sampleFiles", String.join(",", sampleFiles))
                .setOutputPath(serviceStdOut)
                .setErrorPath(serviceStdErr);

        JacsServiceData testServiceData = testServiceDataBuilder.build();
        testServiceData.setId(serviceId);
        return testServiceData;
    }

}

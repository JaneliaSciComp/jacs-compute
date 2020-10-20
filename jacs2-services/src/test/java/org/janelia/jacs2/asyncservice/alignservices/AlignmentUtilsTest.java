package org.janelia.jacs2.asyncservice.alignservices;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.spi.FileSystemProvider;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.mockito.Mockito;

public class AlignmentUtilsTest {

    @Test
    public void convertAffineToInsightMat() throws IOException {
        final String testMat = "src/test/resources/testdata/alignmentUtils/affine.mat";
        final ByteArrayOutputStream outputWriter = new ByteArrayOutputStream();
        Path testOutputPath = Mockito.mock(Path.class);
        File testOutputFile = Mockito.mock(File.class);
        FileSystem testFs = Mockito.mock(FileSystem.class);
        FileSystemProvider testFsProvider = Mockito.mock(FileSystemProvider.class);
        Mockito.when(testOutputPath.toFile()).thenReturn(testOutputFile);
        Mockito.when(testOutputPath.getFileSystem()).thenReturn(testFs);
        Mockito.when(testFs.provider()).thenReturn(testFsProvider);
        Mockito.when(testFsProvider.newOutputStream(testOutputPath)).thenReturn(outputWriter);
        AlignmentUtils.convertAffineMatToInsightMat(Paths.get(testMat), testOutputPath);
        String nl = System.lineSeparator();
        final String expectedOutput = "#Insight Transform File V1.0" + nl +
                "#Transform 0" + nl +
                "Transform: MatrixOffsetTransformBase_double_3_3" + nl +
                "Parameters: 0.7760273859 0.5681118308 -0.08607871711 -0.5312055299 0.7869334512 -0.1875634849 -0.06294143877 0.1666959753 0.8998907077 0 0 0" + nl +
                "FixedParameters: 0 0 0" + nl;
        MatcherAssert.assertThat(outputWriter.toString(), CoreMatchers.equalTo(expectedOutput));
    }
}

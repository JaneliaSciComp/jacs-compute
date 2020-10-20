package org.janelia.jacs2.asyncservice.utils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.hamcrest.MatcherAssert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;

public class FileUtilsTest {

    private static class InputData {
        private final Path dir;
        private final String prefix;
        private final String fileName;
        private final String suffix;
        private final String fileExt;

        InputData(Path dir, String prefix, String fileName, String suffix, String fileExt) {
            this.dir = dir;
            this.prefix = prefix;
            this.fileName = fileName;
            this.suffix = suffix;
            this.fileExt = fileExt;
        }
    }

    @Test
    public void getFilePathWithPrefixAndSuffix() {
        Map<InputData, String> testData = ImmutableMap.<InputData, String>builder()
                .put(new InputData(Paths.get("test"), "prefix", "name", "suffix", ".ext"), "test/prefixnamesuffix.ext")
                .put(new InputData(Paths.get("test"), "prefix", "name", "suffix", "ext"), "test/prefixnamesuffix.ext")
                .put(new InputData(Paths.get("test"), "prefix", "name", "suffix", null), "test/prefixnamesuffix")
                .put(new InputData(Paths.get("test"), "prefix", "name.ext", "suffix", null), "test/prefixnamesuffix")
                .put(new InputData(Paths.get("test"), null, "name.ext", "suffix", "newext"), "test/namesuffix.newext")
                .put(new InputData(Paths.get("test"), "prefix", "name.ext", null, "newext"), "test/prefixname.newext")
                .put(new InputData(Paths.get("test"), "", null, null, "newext"), "test/.newext")
                .build();

        testData.entrySet().forEach(entry -> {
            MatcherAssert.assertThat(FileUtils.getFilePath(
                   entry.getKey().dir,
                   entry.getKey().prefix,
                   entry.getKey().fileName,
                   entry.getKey().suffix,
                   entry.getKey().fileExt
                   ), equalTo(Paths.get(entry.getValue())));
        });
    }

    @Test
    public void replaceFileExt() {
        Map<String, String> testData = ImmutableMap.<String, String>builder()
                .put("test/filenamewithext.oldext", "test/filenamewithext.newext")
                .put("test/filenamenoext", "test/filenamenoext.newext")
                .put("test/filenamewithext.ext1.ext2", "test/filenamewithext.ext1.newext")
                .build();
        testData.entrySet().forEach(entry -> {
            MatcherAssert.assertThat(FileUtils.replaceFileExt(Paths.get(entry.getKey()), "newext"), equalTo(Paths.get(entry.getValue())));
        });
    }

    @Test
    public void commonPath() {
        class TestData {
            final List<String> paths;
            final Optional<String> expectedResult;

            TestData(List<String> paths, Optional<String> expectedResult) {
                this.paths = paths;
                this.expectedResult = expectedResult;
            }
        }
        TestData[] testData = new TestData[] {
                new TestData(ImmutableList.of("/a/b/c/d", "/a/b/c/e", "/a/b/c/f"), Optional.of("/a/b/c")),
                new TestData(ImmutableList.of("/a/c/c/d", "/a/b/c/e", "/a/b/c/f"), Optional.of("/a")),
                new TestData(ImmutableList.of("a/b/c/d", "/a/b/c/e", "/a/b/c/f"), Optional.empty())
        };
        for (TestData td : testData) {
            MatcherAssert.assertThat(FileUtils.commonPath(td.paths), equalTo(td.expectedResult));
        }
    }
}

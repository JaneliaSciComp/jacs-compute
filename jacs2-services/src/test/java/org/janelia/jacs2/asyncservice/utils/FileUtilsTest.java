package org.janelia.jacs2.asyncservice.utils;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

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
           assertThat(FileUtils.getFilePath(
                   entry.getKey().dir,
                   entry.getKey().prefix,
                   entry.getKey().fileName,
                   entry.getKey().suffix,
                   entry.getKey().fileExt
                   ), equalTo(Paths.get(entry.getValue())));
        });
    }

}

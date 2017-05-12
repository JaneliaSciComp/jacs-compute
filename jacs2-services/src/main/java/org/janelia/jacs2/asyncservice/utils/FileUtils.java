package org.janelia.jacs2.asyncservice.utils;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class FileUtils {

    public static Stream<Path> lookupFiles(Path dir, int maxDepth, String pattern) {
        try {
            PathMatcher inputFileMatcher = FileSystems.getDefault().getPathMatcher(pattern);
            return Files.find(dir, maxDepth, (p, a) -> inputFileMatcher.matches(p));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static boolean fileExists(String filepath) {
        return Files.exists(Paths.get(filepath));
    }

    public static boolean fileNotExists(String filepath) {
        return !fileExists(filepath);
    }

    /**
     * Deletes the given directory even if it's non empty.
     *
     * @param dir
     * @throws IOException
     */
    public static void deletePath(Path dir) throws IOException {
        Files.walkFileTree(dir, new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static String getFileNameOnly(Path fp) {
        return getFileNameOnly(fp.toString());
    }

    public static String getFileNameOnly(String fn) {
        return StringUtils.isBlank(fn) ? "" : com.google.common.io.Files.getNameWithoutExtension(fn);
    }

    public static String getFileExtensionOnly(Path fp) {
        return getFileExtensionOnly(fp.toString());
    }

    public static String getFileExtensionOnly(String fn) {
        return StringUtils.isBlank(fn) ? "" : createExtension(com.google.common.io.Files.getFileExtension(fn));
    }

    private static String createExtension(String ext) {
        if (StringUtils.isBlank(ext)) {
            return "";
        } else {
            if (StringUtils.startsWith(ext, ".")) {
                return ext;
            } else {
                return "." + ext;
            }
        }
    }

    public static Path getFilePath(Path dir, String fileName) {
        return dir.resolve(new File(fileName).getName());
    }

    public static Path replaceFileExt(Path filePath, String fileExt) {
        return getFilePath(filePath.getParent(), filePath.toFile().getName(), fileExt);
    }

    public static Path getFilePath(Path dir, String fileName, String fileExt) {
        return getFilePath(dir, null, fileName, null, fileExt);
    }

    public static Path getFilePath(Path dir, String prefix, String fileName, String suffix, String fileExt) {
        String actualFileName = String.format("%s%s%s%s",
                StringUtils.defaultIfBlank(prefix, ""),
                FileUtils.getFileNameOnly(fileName),
                StringUtils.defaultIfBlank(suffix, ""),
                createExtension(fileExt));
        return dir.resolve(actualFileName);
    }

    public static List<String> getTreePathComponentsForId(Number id) {
        return id == null ? Collections.emptyList() : getTreePathComponentsForId(id.toString());
    }

    public static List<String> getTreePathComponentsForId(String id) {
        if (StringUtils.isBlank(id)) {
            return Collections.emptyList();
        }
        String trimmedId = id.trim();
        int idLength = trimmedId.length();
        if (idLength < 7) {
            return ImmutableList.of(trimmedId);
        } else {
            return ImmutableList.of(
                    trimmedId.substring(idLength - 6, idLength - 3),
                    trimmedId.substring(idLength - 3),
                    trimmedId);
        }
    }

}

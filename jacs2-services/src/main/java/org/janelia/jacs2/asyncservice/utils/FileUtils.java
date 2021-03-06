package org.janelia.jacs2.asyncservice.utils;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.StringUtils;
import org.janelia.model.jacs2.domain.IndexedReference;

public class FileUtils {

    public static Stream<Path> lookupFiles(Path dir, int maxDepth, String pattern) {
        try {
            String fileLookupPattern;
            if (StringUtils.isBlank(pattern)) {
                fileLookupPattern = "glob:**/*";
            } else if (!pattern.startsWith("glob:") && !pattern.startsWith("regex:")) {
                // default to glob
                fileLookupPattern = "glob:" + pattern;
            } else {
                fileLookupPattern = pattern;
            }
            PathMatcher inputFileMatcher = FileSystems.getDefault().getPathMatcher(fileLookupPattern);
            return Files.find(dir, maxDepth, (p, a) -> inputFileMatcher.matches(p));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static boolean fileExists(String filepath) {
        return Files.exists(Paths.get(filepath));
    }

    public static boolean fileNotExists(String filepath) {
        return Files.notExists(Paths.get(filepath));
    }

    public static boolean fileExists(Path filepath) {
        return Files.exists(filepath);
    }

    public static boolean fileNotExists(Path filepath) {
        return Files.notExists(filepath);
    }

    public static Path createSubDirs(Path dir, String subDir) {
        Path subDirPath = dir.resolve(subDir);
        try {
            java.nio.file.Files.createDirectories(subDirPath);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create job subdirectory " + subDirPath, e);
        }
        return subDirPath;
    }

    /**
     * Deletes the given directory even if it's non empty.
     *
     * @param dir
     * @throws IOException
     */
    public static void deletePath(Path dir) throws IOException {
        if (dir == null) {
            return; // do nothing
        }
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

    public static String getFileName(String fn) {
        return StringUtils.isBlank(fn) ? "" : Paths.get(fn).getFileName().toString();
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

    public static String getParent(String fp) {
        if (StringUtils.isBlank(fp)) {
            return "";
        } else {
            Path p = Paths.get(fp).getParent();
            return p != null ? p.toString() : "";
        }
    }

    private static String createExtension(String ext) {
        if (StringUtils.isBlank(ext)) {
            return "";
        } else {
            return StringUtils.prependIfMissing(ext, ".");
        }
    }

    public static Path getSubDir(Path parent, String dirName) {
        if (parent == null) {
            return Paths.get(dirName);
        } else {
            return parent.resolve(dirName);
        }
    }

    public static Path getFilePath(Path dir, String fileName) {
        return dir != null ? dir.resolve(getFileName(fileName)) : Paths.get(getFileName(fileName));
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
        return dir != null ? dir.resolve(actualFileName) : Paths.get(actualFileName);
    }

    public static Path getDataPath(String dataRootDir, Number dataInstanceId) {
        List<String> pathComponents = FileUtils.getTreePathComponentsForId(dataInstanceId);
        return Paths.get(dataRootDir, pathComponents.toArray(new String[pathComponents.size()]));
    }

    public static Path getDataPath(String dataRootDir, String dataInstanceId) {
        List<String> pathComponents = FileUtils.getTreePathComponentsForId(dataInstanceId);
        return Paths.get(dataRootDir, pathComponents.toArray(new String[pathComponents.size()]));
    }

    public static List<String> getTreePathComponentsForId(Number id) {
        return id == null ? Collections.emptyList() : getTreePathComponentsForId(id.toString());
    }

    private static List<String> getTreePathComponentsForId(String id) {
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

    public static Optional<String> commonPath(List<String> paths) {
        List<String> commonPathComponents = new ArrayList<>();
        String[][] folders = new String[paths.size()][];
        IndexedReference.indexListContent(paths, (pos, p) -> new IndexedReference<>(p, pos))
                .forEach(indexedPath -> {
                    folders[indexedPath.getPos()] = indexedPath.getReference().split("/");
                });
        for (int j = 0; j < folders[0].length; j++) {
            String thisFolder = folders[0][j]; // grab the next folder name in the first path
            boolean allMatched = true; // assume all have matched in case there are no more paths
            for(int i = 1; i < folders.length && allMatched; i++) { //look at the other paths
                if(folders[i].length < j) { // if there is no folder here
                    allMatched = false; // no match
                    break; // stop looking because we've gone as far as we can
                }
                //otherwise
                allMatched &= folders[i][j].equals(thisFolder); //check if it matched
            }
            if (allMatched) {
                commonPathComponents.add(thisFolder);
            } else {
                break; // stop looking
            }
        }
        return commonPathComponents.isEmpty() ? Optional.empty() : Optional.of(String.join("/", commonPathComponents));
    }
}

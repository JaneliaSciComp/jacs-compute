package org.janelia.jacs2.asyncservice.utils;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.model.jacs2.domain.IndexedReference;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Stream;

/**
 * Moved from JACSv1.
 * TODO: factor this out and use modern file utility methods
 */
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


    //------------------------------------------------------------------------------------------------------------------
    // Below utility methods come from JACSv1.
    // TODO: replace use of these methods with more standard methods from jakarta-commons, etc.
    //------------------------------------------------------------------------------------------------------------------

    /**
     * Write the given string to a file, overwriting the current content.
     * @param file
     * @param s
     */
    public static void writeStringToFile(File file, String s) throws IOException {
        writeStringToFile(file, s, false);
    }

    /**
     * Write the given string to a file.
     * @param file
     * @param s
     */
    public static void writeStringToFile(File file, String s, boolean append) throws IOException {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter(file, append));
            out.write(s);
        }
        catch (Exception e) {
            throw new IOException("Could not write to file: "+file.getAbsolutePath());
        }
        finally {
            try {
                out.close();
            }
            catch (Exception e) {
                throw new IOException("Could not close file: "+file.getAbsolutePath(),e);
            }
        }
    }

    /**
     * Deletes a directory recursively.
     *
     * @param directory  directory to delete
     * @throws IOException in case deletion is unsuccessful
     */
    public static void deleteDirectory(File directory) throws IOException {
        if (!directory.exists()) {
            return;
        }

        if (!isSymlink(directory)) {
            cleanDirectory(directory);
        }

        if (!directory.delete()) {
            String message =
                    "Unable to delete directory " + directory + ".";
            throw new IOException(message);
        }
    }

    /**
     * Cleans a directory without deleting it.
     *
     * @param directory directory to clean
     * @throws IOException in case cleaning is unsuccessful
     */
    public static void cleanDirectory(File directory) throws IOException {
        if (!directory.exists()) {
            String message = directory + " does not exist";
            throw new IllegalArgumentException(message);
        }

        if (!directory.isDirectory()) {
            String message = directory + " is not a directory";
            throw new IllegalArgumentException(message);
        }

        File[] files = directory.listFiles();
        if (files == null) {  // null if security restricted
            throw new IOException("Failed to list contents of " + directory);
        }

        IOException exception = null;
        for (File file : files) {
            try {
                forceDelete(file);
            } catch (IOException ioe) {
                exception = ioe;
            }
        }

        if (null != exception) {
            throw exception;
        }
    }

    //-----------------------------------------------------------------------
    /**
     * Deletes a file. If file is a directory, delete it and all sub-directories.
     * <p>
     * The difference between File.delete() and this method are:
     * <ul>
     * <li>A directory to be deleted does not have to be empty.</li>
     * <li>You get exceptions when a file or directory cannot be deleted.
     *      (java.io.File methods returns a boolean)</li>
     * </ul>
     *
     * @param file  file or directory to delete, must not be <code>null</code>
     * @throws NullPointerException if the directory is <code>null</code>
     * @throws FileNotFoundException if the file was not found
     * @throws IOException in case deletion is unsuccessful
     */
    public static void forceDelete(File file) throws IOException {
        if (file.isDirectory()) {
            deleteDirectory(file);
        } else {
            boolean filePresent = file.exists();
            if (!file.delete()) {
                if (!filePresent){
                    throw new FileNotFoundException("File does not exist: " + file);
                }
                String message =
                        "Unable to delete file: " + file;
                throw new IOException(message);
            }
        }
    }

    /**
     * Determines whether the specified file is a Symbolic Link rather than an actual file.
     * <p>
     * Will not return true if there is a Symbolic Link anywhere in the path,
     * only if the specific file is.
     *
     * @param file the file to check
     * @return true if the file is a Symbolic Link
     * @throws IOException if an IO error occurs while checking the file
     * @since Commons IO 2.0
     */
    public static boolean isSymlink(File file) throws IOException {
        if (file == null) {
            throw new NullPointerException("File must not be null");
        }
        File fileInCanonicalDir;
        if (file.getParent() == null) {
            fileInCanonicalDir = file;
        } else {
            File canonicalDir = file.getParentFile().getCanonicalFile();
            fileInCanonicalDir = new File(canonicalDir, file.getName());
        }

        return !fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile());
    }


    /**
     * @param  dir  the directory to examine.
     *
     * @return the child files of the given directory, sorted by name.
     *         An empty list is returned if the directory does not exist or is not a directory.
     */
    public static List<File> getOrderedFilesInDir(File dir) {

        List<File> orderedFiles = null;

        if ((dir != null) && dir.isDirectory()) {
            final File[] files = dir.listFiles();
            if (files != null) {
                orderedFiles = Arrays.asList(files);
            }
        }

        if (orderedFiles == null) {
            orderedFiles = new ArrayList<File>();
        } else {
            sortFilesByName(orderedFiles);
        }

        return orderedFiles;
    }

    /**
     * Sort the given list of files in place, by name.
     * @param files
     */
    public static void sortFilesByName(List<File> files) {
        Collections.sort(files, new Comparator<File>() {
            @Override
            public int compare(File file1, File file2) {
                return file1.getName().compareTo(file2.getName());
            }
        });
    }

    /**
     * Use this to violate a cherished rule of Java EE: no reading from the file system from within the server.
     * Using this routine will at least allow one to do so in a centralized way. ;-)
     *
     * Format Notes: any line beginning with pound sign is a comment and is skipped;  any line consisting
     * of white space or which is empty is also skipped.  Otherwise, all lines are epxected to be parseable as
     * Java longs.
     *
     * @param idListFilePath full path to some file in format described above.
     * @return the resulting list of ids.
     * @throws Exception thrown called methods, or if file cannot be opened.
     */
    public static List<Long> getIdsFromFile(String idListFilePath) throws Exception {
        List<Long> ids = new ArrayList<>();
        BufferedReader br = null;
        try {
            File f = new File(idListFilePath);
            if (! (f.canRead()  &&  f.isFile())) {
                throw new IllegalArgumentException("Cannot open file " + idListFilePath);
            }
            br = new BufferedReader(new FileReader(f));
            String inline = null;
            while (null != (inline = br.readLine())) {
                if (inline.trim().length() == 0) {
                    continue;
                }
                if (inline.startsWith("#")) {
                    continue;
                }
                Long id = Long.parseLong(inline.trim());
                ids.add(id);
            }

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            if (br != null)
                br.close();
        }
        return ids;
    }

    public static String getPrefix(String filepath) {
        if (filepath==null) return null;
        File file = new File(filepath);
        String name = file.getName();
        int index = name.indexOf('.');
        if (index<0) return name;
        return name.substring(0, index);
    }

    public static String getFilePrefix(String filepath) {
        if (filepath==null) return null;
        File file = new File(filepath);
        String name = file.getName();
        int index = name.lastIndexOf('.');
        if (index<0) return name;
        return name.substring(0, index);
    }
//------------------------------------------------------------------------------------------------------------------
    // Below utility methods come from JACSv1 shared model FileUtils.
    // TODO: replace use of these methods with more standard methods from jakarta-commons, etc.
    //------------------------------------------------------------------------------------------------------------------

    public static File[] getFilesWithPrefixes(File directory, final String... prefixes) {
        return directory.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                for(String prefix : prefixes) {
                    if (name.startsWith(prefix)) {
                        return true;
                    }
                }
                return false;
            }
        });
    }

    public static File[] getFilesWithSuffixes(File directory, final String... suffixes) {
        return directory.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                for(String suffix : suffixes) {
                    if (name.endsWith(suffix)) {
                        return true;
                    }
                }
                return false;
            }
        });
    }

    public static File[] getSubDirectories(File dir) {
        if (!dir.isDirectory()) throw new IllegalArgumentException("Given file is not a directory");
        return dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isDirectory();
            }
        });
    }


}

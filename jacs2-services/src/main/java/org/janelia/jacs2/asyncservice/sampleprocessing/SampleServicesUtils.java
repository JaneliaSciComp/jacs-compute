package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.janelia.model.jacs2.domain.enums.FileType;
import org.janelia.model.jacs2.domain.interfaces.HasRelativeFiles;
import org.janelia.model.jacs2.domain.sample.FileGroup;
import org.janelia.model.jacs2.domain.sample.Image;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.model.jacs2.EntityFieldValueHandler;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SampleServicesUtils {

    static final String DEFAULT_WORKING_LSMS_SUBDIR = "Temp";

    static Path getImageDataPath(String destRootDirName, String destSubDirName, String objective, String area) {
        Path imageDataPath = Paths.get(destRootDirName,
                StringUtils.defaultIfBlank(destSubDirName, ""),
                StringUtils.defaultIfBlank(objective, ""),
                StringUtils.defaultIfBlank(area, "")
        );
        return imageDataPath;
    }

    static Path getImageFile(String destRootDirName, String destSubDirName, String objective, String area, Image image) {
        String fileName = new File(image.getFilepath()).getName();
        if (fileName.endsWith(".bz2")) {
            fileName = fileName.substring(0, fileName.length() - ".bz2".length());
        } else if (fileName.endsWith(".gz")) {
            fileName = fileName.substring(0, fileName.length() - ".gz".length());
        }
        return getImageDataPath(destRootDirName, destSubDirName, objective, area).resolve(fileName);
    }

    static Path getImageMetadataFile(String destRootDirName, String destSubDirName, String objective, String area, File imageFile) {
        return getImageDataPath(destRootDirName, destSubDirName, objective, area).resolve(imageFile.getName().replaceAll("\\s+", "_") + ".json");
    }

    /**
     * Updates the files from the fileGroups.
     *
     * @param objectWithFiles object to be updated
     * @param fileGroups list of filegroups
     * @return true if the object has been changed
     */
    public static Map<String, EntityFieldValueHandler<?>> updateFiles(HasRelativeFiles objectWithFiles, List<FileGroup> fileGroups) {
        return fileGroups.stream()
                .flatMap(group -> group.getFiles().entrySet().stream())
                .map(fileTypeEntry -> objectWithFiles.setFileName(fileTypeEntry.getKey(), fileTypeEntry.getValue()))
                .reduce(new LinkedHashMap<>(), (r1, r2) -> {
                    Map<String, EntityFieldValueHandler<?>> result = new LinkedHashMap<>();
                    result.putAll(r1);
                    result.putAll(r2);
                    return result;
                });
    }

    public static List<FileGroup> createFileGroups(String groupFilePath, List<String> filepaths) {
        return streamFileGroups(groupFilePath, filepaths).collect(Collectors.toList());
    }

    public static Stream<FileGroup> streamFileGroups(String groupFilePath, List<String> filepaths) {
        class FileNameStruct {
            String fullFilepath;
            String nameWithExt;
            String name;
            String key;
            String type;
            FileType fileType;
            String ext;
        }
        Map<String, FileGroup> groups = new LinkedHashMap<>();
        filepaths.stream()
                .map(fp -> {
                    File f = new File(fp);
                    String fileName = f.getName();
                    int lastDotIndex = fileName.lastIndexOf('.');
                    if (lastDotIndex < 0) {
                        return null;
                    } else {
                        FileNameStruct fn = new FileNameStruct();
                        fn.fullFilepath = fp;
                        fn.nameWithExt = fileName;
                        fn.name = fileName.substring(0, lastDotIndex);
                        int keyTypeSeparatorIndex = fn.name.lastIndexOf('_');
                        if (keyTypeSeparatorIndex >= 0) {
                            fn.key = fn.name.substring(0, keyTypeSeparatorIndex);
                            fn.type = fn.name.substring(keyTypeSeparatorIndex + 1);
                        }
                        fn.ext = fileName.substring(lastDotIndex + 1);
                        return fn;
                    }
                })
                .filter(fn -> fn != null)
                .filter(fn -> StringUtils.isNotBlank(fn.type)) // filter out files that don't have a type
                .filter(fn -> !"properties" .equals(fn.ext)) // filter out .properties
                .filter(fn -> !fn.nameWithExt.endsWith(".lsm.metadata")) // filter out old style perl metadata
                .map(fn -> {
                    fn.fileType = getFileType(fn.ext, fn.type);
                    return fn;
                })
                .filter(fn -> fn.fileType != null)
                .forEach(fn -> {
                    FileGroup group = groups.get(fn.key);
                    if (group == null) {
                        group = new FileGroup(fn.key);
                        group.setFilepath(StringUtils.defaultIfBlank(groupFilePath, fn.fullFilepath));
                        groups.put(fn.key, group);
                    }
                    group.setFileName(fn.fileType, fn.fullFilepath);
                });
        return groups.values().stream();
    }

    private static FileType getFileType(String ext, String type) {
        if ("png" .equals(ext)) {
            switch (type) {
                case "all":
                    return FileType.AllMip;
                case "reference":
                    return FileType.ReferenceMip;
                case "signal":
                    return FileType.SignalMip;
                case "signal1":
                    return FileType.Signal1Mip;
                case "signal2":
                    return FileType.Signal2Mip;
                case "signal3":
                    return FileType.Signal3Mip;
                case "refsignal1":
                    return FileType.RefSignal1Mip;
                case "refsignal2":
                    return FileType.RefSignal2Mip;
                case "refsignal3":
                    return FileType.RefSignal3Mip;
            }
        } else if ("mp4" .equals(ext)) {
            switch (type) {
                case "all":
                case "movie":
                    return FileType.AllMovie;
                case "reference":
                    return FileType.ReferenceMovie;
                case "signal":
                    return FileType.SignalMovie;
            }
        }
        return null;
    }

    public static FileGroup normalize(FileGroup fileGroup) {
        Map<FileType, Path> allFilesWithFullPath = fileGroup.getFiles().entrySet().stream()
                .map(fileEntry -> Maps.immutableEntry(fileEntry.getKey(), fileGroup.getFullFilePath(fileEntry.getKey())))
                .collect(Collectors.toMap(fe -> fe.getKey(), fe -> fe.getValue()));
        Optional<String> groupCommonPath = FileUtils.commonPath(allFilesWithFullPath.values().stream().map(Path::toString).collect(Collectors.toList()));
        FileGroup normalizedGroup;
        if (groupCommonPath.isPresent() && !groupCommonPath.get().equals(fileGroup.getKey())) {
            normalizedGroup = new FileGroup(fileGroup.getKey());
            normalizedGroup.setFilepath(groupCommonPath.get());
            allFilesWithFullPath.forEach((ft, path) -> {
                normalizedGroup.setFileName(ft, path.toString());
            });
        } else {
            // either it could not find a common path or it could not do better than already is
            normalizedGroup = fileGroup;
        }
        return normalizedGroup;
    }

}

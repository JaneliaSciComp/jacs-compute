package org.janelia.jacs2.asyncservice.dataimport;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.model.domain.enums.FileType;

import java.util.Set;

public class FileTypeHelper {

    private static final Set<String> LOSSLESS_IMAGE_EXTENSIONS = ImmutableSet.of(
            ".lsm", ".tif", ".raw", ".v3draw", ".vaa3draw", ".v3dpbd"
    );

    private static final Set<String> UNCLASSIFIED_2D_EXTENSIONS = ImmutableSet.of(
            ".png", ".jpg", ".tif", ".img", ".gif"
    );


    static FileType getFileTypeByExtension(String fileArtifact, FileType defaultFileType) {
        String fileArtifactExt = FileUtils.getFileExtensionOnly(fileArtifact);
        if (StringUtils.endsWith(fileArtifact,"_all.png")) {
            return FileType.AllMip;
        } else if (StringUtils.endsWith(fileArtifact,"_reference.png")) {
            return FileType.ReferenceMip;
        } else if (StringUtils.endsWith(fileArtifact,"_signal.png")) {
            return FileType.SignalMip;
        } else if (StringUtils.endsWith(fileArtifact,"_signal1.png")) {
            return FileType.Signal1Mip;
        } else if (StringUtils.endsWith(fileArtifact,"_signal2.png")) {
            return FileType.Signal2Mip;
        } else if (StringUtils.endsWith(fileArtifact,"_signal3.png")) {
            return FileType.Signal3Mip;
        } else if (StringUtils.endsWith(fileArtifact,"_refsignal1.png")) {
            return FileType.RefSignal1Mip;
        } else if (StringUtils.endsWith(fileArtifact,"_refsignal2.png")) {
            return FileType.RefSignal2Mip;
        } else if (StringUtils.endsWith(fileArtifact,"_refsignal3.png")) {
            return FileType.RefSignal3Mip;
        } else if (StringUtils.endsWith(fileArtifact,"_all.mp4")) {
            return FileType.AllMovie;
        } else if (StringUtils.endsWith(fileArtifact,"_reference.mp4")) {
            return FileType.ReferenceMovie;
        } else if (StringUtils.endsWith(fileArtifact,"_signal.mp4")) {
            return FileType.SignalMovie;
        } else if (StringUtils.endsWith(fileArtifact,"_movie.mp4")) {
            return FileType.AllMovie;
        } else if (StringUtils.isNotBlank(fileArtifactExt)) {
            if (LOSSLESS_IMAGE_EXTENSIONS.contains(fileArtifactExt.toLowerCase())) {
                return FileType.LosslessStack;
            } else if (UNCLASSIFIED_2D_EXTENSIONS.contains(fileArtifactExt.toLowerCase())) {
                return FileType.Unclassified2d;
            } else {
                return defaultFileType;
            }
        } else {
            return defaultFileType;
        }
    }

}

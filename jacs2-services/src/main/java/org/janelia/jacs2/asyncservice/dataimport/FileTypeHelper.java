package org.janelia.jacs2.asyncservice.dataimport;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.model.domain.enums.FileType;

import java.util.EnumSet;
import java.util.Set;

class FileTypeHelper {

    private static final Set<String> LOSSLESS_IMAGE_EXTENSIONS = ImmutableSet.of(
            ".lsm", ".tif", ".raw", ".v3draw", ".vaa3draw", ".v3dpbd"
    );

    private static final Set<String> UNCLASSIFIED_2D_EXTENSIONS = ImmutableSet.of(
            ".png", ".jpg", ".tif", ".img", ".gif"
    );

    private static final Set<String> VISUALLYLOSSLESS_IMAGE_EXTENSIONS = ImmutableSet.of(
            ".h5j", ".nrrd"
    );

    /**
     * @param fileArtifact
     * @return all filetype candidates if a certain suffix is found.
     */
    static Set<FileType> getFileTypeByExtension(String fileArtifact) {
        String fileArtifactExt = FileUtils.getFileExtensionOnly(fileArtifact);
        if (StringUtils.endsWith(fileArtifact,"_all.png")) {
            return EnumSet.of(FileType.AllMip, FileType.SignalMip);
        } else if (StringUtils.endsWith(fileArtifact,"_reference.png")) {
            return EnumSet.of(FileType.ReferenceMip);
        } else if (StringUtils.endsWith(fileArtifact,"_signal.png")) {
            return EnumSet.of(FileType.SignalMip);
        } else if (StringUtils.endsWith(fileArtifact,"_signal1.png")) {
            return EnumSet.of(FileType.Signal1Mip);
        } else if (StringUtils.endsWith(fileArtifact,"_signal2.png")) {
            return EnumSet.of(FileType.Signal2Mip);
        } else if (StringUtils.endsWith(fileArtifact,"_signal3.png")) {
            return EnumSet.of(FileType.Signal3Mip);
        } else if (StringUtils.endsWith(fileArtifact,"_signal4.png")) {
            return EnumSet.of(FileType.Signal4Mip);
        } else if (StringUtils.endsWith(fileArtifact,"_refsignal1.png")) {
            return EnumSet.of(FileType.RefSignal1Mip);
        } else if (StringUtils.endsWith(fileArtifact,"_refsignal2.png")) {
            return EnumSet.of(FileType.RefSignal2Mip);
        } else if (StringUtils.endsWith(fileArtifact,"_refsignal3.png")) {
            return EnumSet.of(FileType.RefSignal3Mip);
        } else if (StringUtils.endsWith(fileArtifact,"_all.mp4")) {
            return EnumSet.of(FileType.AllMovie);
        } else if (StringUtils.endsWith(fileArtifact,"_reference.mp4")) {
            return EnumSet.of(FileType.ReferenceMovie);
        } else if (StringUtils.endsWith(fileArtifact,"_signal.mp4")) {
            return EnumSet.of(FileType.SignalMovie);
        } else if (StringUtils.endsWith(fileArtifact,"_movie.mp4")) {
            return EnumSet.of(FileType.AllMovie);
        } else if (StringUtils.isNotBlank(fileArtifactExt)) {
            if (LOSSLESS_IMAGE_EXTENSIONS.contains(fileArtifactExt.toLowerCase())) {
                return EnumSet.of(FileType.LosslessStack);
            } else if (VISUALLYLOSSLESS_IMAGE_EXTENSIONS.contains(fileArtifactExt.toLowerCase())) {
                return EnumSet.of(FileType.VisuallyLosslessStack);
            } else if (UNCLASSIFIED_2D_EXTENSIONS.contains(fileArtifactExt.toLowerCase())) {
                return EnumSet.of(FileType.Unclassified2d);
            } else {
                return EnumSet.noneOf(FileType.class);
            }
        } else {
            return EnumSet.noneOf(FileType.class);
        }
    }

}

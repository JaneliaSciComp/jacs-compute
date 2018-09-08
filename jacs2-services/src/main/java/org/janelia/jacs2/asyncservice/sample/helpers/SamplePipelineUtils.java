package org.janelia.jacs2.asyncservice.sample.helpers;

import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.utils.ArchiveUtils;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.interfaces.HasFiles;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SamplePipelineUtils {

    public static String getLSMPrefix(HasFiles lsm) {
        String filepath = DomainUtils.getFilepath(lsm, FileType.LosslessStack);
        String decompressedFilepath = ArchiveUtils.getDecompressedFilepath(filepath);
        return FileUtils.getFilePrefix(decompressedFilepath);
    }

}

package org.janelia.it.jacs.model.domain.interfaces;

import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.jacs2.model.EntityFieldValueHandler;

import java.util.Map;

/**
 * Any object implementing this interface has the option of associated files of specific types. 
 * 
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public interface HasFiles {
    Map<FileType, String> getFiles();
    String getFileName(FileType fileType);
    default boolean hasFileName(FileType fileType) {
        String fn = getFileName(fileType);
        return fn != null && fn.trim().length() > 0;
    }
    Map<String, EntityFieldValueHandler<?>> setFileName(FileType fileType, String fileName);
    Map<String, EntityFieldValueHandler<?>> removeFileName(FileType fileType);
}

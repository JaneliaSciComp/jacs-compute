package org.janelia.it.jacs.model.domain.interfaces;

import org.janelia.it.jacs.model.domain.enums.FileType;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Any object implementing this interface has the option of associated files of specific types. 
 * 
 * The file paths given by getFiles() may be relative to the overall root filepath given by getFilepath().
 *
 * The best way to deal with these relative filepaths is by using the relevant DomainModelUtils methods.
 */
public interface HasRelativeFiles extends HasFilepath, HasFiles {
    default Path getFullFilePath(FileType fileType) {
        if (getFilepath() != null) {
            return Paths.get(getFilepath(), getFileName(fileType));
        } else {
            return Paths.get(getFileName(fileType));
        }
    }

}

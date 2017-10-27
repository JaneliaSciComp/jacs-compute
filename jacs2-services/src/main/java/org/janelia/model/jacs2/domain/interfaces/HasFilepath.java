package org.janelia.model.jacs2.domain.interfaces;

/**
 * Any object implementing this interface has an associated file or directory on disk.  
 * 
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public interface HasFilepath {
    String getFilepath();
    default boolean hasFilepath() {
        return getFilepath() != null && getFilepath().trim().length() > 0;
    }
}

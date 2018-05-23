package org.janelia.jacs2.utils;

/**
 * Shared utility methods for dealing with archived files.
 * 
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ArchiveUtils {

    private static final String EXTENSION_BZIP2 = ".bz2";
    private static final String EXTENSION_GZIP = ".gz";

    /**
     * Returns a filepath or filename with any of the following extensions removed:
     * .bz2 .gz
     * If none of the extensions matches, the original parameter is returned unchanged.
     * @param filepath a filepath or filename
     * @return filepath stripped of compression extensions
     */
    public static String getDecompressedFilepath(String filepath) {
        if (filepath==null) return null;
        if (filepath.endsWith(EXTENSION_BZIP2)) {
            return filepath.substring(0, filepath.length()-EXTENSION_BZIP2.length());
        }
        if (filepath.endsWith(EXTENSION_GZIP)) {
            return filepath.substring(0, filepath.length()-EXTENSION_GZIP.length());
        }
        return filepath;
    }
}

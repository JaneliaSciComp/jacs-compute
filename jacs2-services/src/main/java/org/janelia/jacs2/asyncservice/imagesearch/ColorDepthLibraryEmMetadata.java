package org.janelia.jacs2.asyncservice.imagesearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.ObjectMapperFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * Optional user-provided metadata for a color depth library.
 *
 * This represents a emdataset.json that can optionally live in the root library and associate the files with
 * EMDataSet objects.
 */
public class ColorDepthLibraryEmMetadata {

    private static final String METADATA_FILENAME = "emdataset.json";

    private File file;
    private String name;
    private String version;

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    /**
     * Copied from EMDataSet.
     * @return
     */
    public String getDataSetIdentifier() {
        StringBuilder sb = new StringBuilder();
        sb.append(getName());
        if (StringUtils.isNotBlank(getVersion())) {
            sb.append(":v");
            sb.append(getVersion());
        }
        return sb.toString();
    }

    /**
     * Read LSM metadata from the given library path
     * @param path path to the folder structure containing the library
     * @return parsed metadata or null if no file exists
     * @throws IOException if the file cannot be read
     */
    public static ColorDepthLibraryEmMetadata fromLibraryPath(File path) throws IOException {
        File file = new File(path, METADATA_FILENAME);
        if (!file.exists()) {
            return null;
        }
        ObjectMapper objectMapper = ObjectMapperFactory.instance().getDefaultObjectMapper();
        ColorDepthLibraryEmMetadata m = objectMapper.readValue(Files.readAllBytes(file.toPath()), ColorDepthLibraryEmMetadata.class);
        m.setFile(file);
        return m;
    }

}

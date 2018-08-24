package org.janelia.jacs2.dataservice.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.utils.FileUtils;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StoragePathURI {

    private static final Pattern STORAGE_PATH_URI_PATTERN = Pattern.compile("(?<schema>.+://)?(?<storagePath>.*)");

    private final String schema;
    private final String storagePath;

    @JsonCreator
    public StoragePathURI(@JsonProperty("storagePath") String storagePath) {
        if (StringUtils.isBlank(storagePath)) {
            this.schema = "";
            this.storagePath = "";
        } else {
            Matcher m = STORAGE_PATH_URI_PATTERN.matcher(storagePath);
            if (m.matches()) {
                this.schema = StringUtils.defaultIfBlank(m.group("schema"), "");
                this.storagePath = m.group("storagePath");
            } else {
                this.schema = "";
                this.storagePath = storagePath;
            }
        }
    }

    private StoragePathURI(String schema, String storagePath) {
        this.schema = schema;
        this.storagePath = storagePath;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public Optional<StoragePathURI> getParent() {
        if (isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(new StoragePathURI(schema, FileUtils.getParent(this.storagePath)));
        }
    }

    public Optional<StoragePathURI> resolve(String childPath) {
        if (isEmpty()) {
            return Optional.empty();
        } else {
            StringBuilder childPathURIBuilder = new StringBuilder();
            childPathURIBuilder.append(storagePath)
                    .append('/')
                    .append(childPath);
            return Optional.of(new StoragePathURI(schema, childPathURIBuilder.toString()));
        }
    }

    public boolean isEmpty() {
        return StringUtils.isEmpty(storagePath);
    }

    private String stringify(String schemaValue, String pathValue) {
        return StringUtils.defaultIfBlank(schemaValue, "") + pathValue;
    }

    @JsonValue
    @Override
    public String toString() {
        if (isEmpty()) {
            return "";
        } else {
            return stringify(schema, storagePath);
        }
    }
}

package org.janelia.jacs2.asyncservice.dataimport;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacs2.dataservice.storage.StorageEntryInfo;

import java.nio.file.Path;

/**
 * Add path information to StorageEntryInfo.
 */
class StorageLocation {

    private Path absolutePath;
    private Path relativePath;
    private StorageEntryInfo storageInfo;

    StorageLocation(Path absolutePath, Path relativePath, StorageEntryInfo storageInfo) {
        this.absolutePath = absolutePath;
        this.relativePath = relativePath;
        this.storageInfo = storageInfo;
    }

    Path getAbsolutePath() {
        return absolutePath;
    }

    Path getRelativePath() {
        return relativePath;
    }

    StorageEntryInfo getStorageInfo() {
        return storageInfo;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("absolutePath", absolutePath)
                .append("relativePath", relativePath)
                .append("storageInfo", storageInfo)
                .toString();
    }
}
package org.janelia.jacs2.asyncservice.files;

import org.janelia.jacsstorage.clients.api.JadeStorageService;
import org.janelia.jacsstorage.clients.api.StorageObject;
import org.janelia.jacsstorage.clients.api.StorageObjectNotFoundException;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Convenience object for combining Jade access to a stored object, with the object itself.
 */
public class JadeObject {

    private JadeStorageService jadeStorage;
    private StorageObject storageObject;

    public JadeObject(JadeStorageService jadeStorage, StorageObject storageObject) {
        this.jadeStorage = jadeStorage;
        this.storageObject = storageObject;
    }

    public JadeStorageService getJadeStorage() {
        return jadeStorage;
    }

    public StorageObject getStorageObject() {
        return storageObject;
    }

    public List<JadeObject> getSubdirs() throws StorageObjectNotFoundException {
        return jadeStorage.getChildren(storageObject.getLocation(), storageObject.getAbsolutePath(), true)
                .stream().map(this::resolve).collect(Collectors.toList());
    }

    public JadeObject resolve(StorageObject childObject) {
        return new JadeObject(jadeStorage, childObject);
    }
}

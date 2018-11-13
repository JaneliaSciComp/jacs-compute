package org.janelia.model.service;

public enum JacsServiceState {
    CREATED,
    QUEUED,
    DISPATCHED,
    RUNNING,
    WAITING_FOR_DEPENDENCIES,
    CANCELED,
    TIMEOUT,
    ERROR,
    SUCCESSFUL,
    SUSPENDED,
    RESUMED,
    RETRY;
}

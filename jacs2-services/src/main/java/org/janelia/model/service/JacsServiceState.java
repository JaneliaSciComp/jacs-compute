package org.janelia.model.service;

public enum JacsServiceState {
    CREATED, QUEUED, DISPATCHED, RUNNING, SUSPENDED, CANCELED, TIMEOUT, ERROR, SUCCESSFUL, ARCHIVED
}

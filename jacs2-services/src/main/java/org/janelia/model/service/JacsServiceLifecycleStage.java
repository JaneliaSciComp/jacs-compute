package org.janelia.model.service;

public enum JacsServiceLifecycleStage {
    START_PROCESSING,
    SUCCESSFUL_PROCESSING,
    FAILED_PROCESSING,
    SUSPEND_PROCESSING,
    RESUME_PROCESSING,
    RETRY_PROCESSING
}

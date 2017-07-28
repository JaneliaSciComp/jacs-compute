package org.janelia.it.jacs.model.domain.sample;

public enum SampleProcessingStatus {
    New,
    Scheduled,
    Queued,
    Processing,
    Complete,
    Error,
    Retired
}

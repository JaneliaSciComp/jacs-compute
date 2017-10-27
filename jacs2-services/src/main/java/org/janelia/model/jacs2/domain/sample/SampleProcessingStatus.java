package org.janelia.model.jacs2.domain.sample;

public enum SampleProcessingStatus {
    New,
    Scheduled,
    Queued,
    Processing,
    Complete,
    Error,
    Retired
}

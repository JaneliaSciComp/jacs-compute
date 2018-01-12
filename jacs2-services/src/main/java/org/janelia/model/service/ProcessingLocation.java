package org.janelia.model.service;

import org.janelia.jacs2.asyncservice.qualifier.*;

import java.lang.annotation.Annotation;

public enum ProcessingLocation {
    LOCAL(LocalJob.class),
    SGE_DRMAA(SGEDrmaaJob.class),
    LSF_JAVA(LSFJavaJob.class),
    LSF_DRMAA(LSFDrmaaJob.class),
    ;

    private final Class<? extends Annotation> processingAnnotationClass;

    ProcessingLocation(Class<? extends Annotation> processingAnnotationClass) {
        this.processingAnnotationClass = processingAnnotationClass;
    }

    public Class<? extends Annotation> getProcessingAnnotationClass() {
        return processingAnnotationClass;
    }

}

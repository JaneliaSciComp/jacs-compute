package org.janelia.jacs2.model.jacsservice;

import org.janelia.jacs2.asyncservice.qualifier.LSFDrmaaJob;
import org.janelia.jacs2.asyncservice.qualifier.LSFPACJob;
import org.janelia.jacs2.asyncservice.qualifier.LocalJob;
import org.janelia.jacs2.asyncservice.qualifier.SGEDrmaaJob;

import java.lang.annotation.Annotation;

public enum ProcessingLocation {
    LOCAL(LocalJob.class),
    SGE_DRMAA(SGEDrmaaJob.class),
    LSF_DRMAA(LSFDrmaaJob.class),
    LSF_PAC(LSFPACJob.class)
    ;

    private final Class<? extends Annotation> processingAnnotationClass;

    ProcessingLocation(Class<? extends Annotation> processingAnnotationClass) {
        this.processingAnnotationClass = processingAnnotationClass;
    }

    public Class<? extends Annotation> getProcessingAnnotationClass() {
        return processingAnnotationClass;
    }

}

package org.janelia.jacs2.model.jacsservice;

import org.janelia.jacs2.asyncservice.qualifier.LSFArrayClusterJob;
import org.janelia.jacs2.asyncservice.qualifier.LSFClusterJob;
import org.janelia.jacs2.asyncservice.qualifier.LocalJob;
import org.janelia.jacs2.asyncservice.qualifier.SGEClusterJob;

import java.lang.annotation.Annotation;

public enum ProcessingLocation {
    LOCAL(LocalJob.class),
    SGE_CLUSTER(SGEClusterJob.class),
    LSF_ARRAY_CLUSTER(LSFArrayClusterJob.class),
    LSF_CLUSTER(LSFClusterJob.class);

    private final Class<? extends Annotation> processingAnnotationClass;

    ProcessingLocation(Class<? extends Annotation> processingAnnotationClass) {
        this.processingAnnotationClass = processingAnnotationClass;
    }

    public Class<? extends Annotation> getProcessingAnnotationClass() {
        return processingAnnotationClass;
    }

}

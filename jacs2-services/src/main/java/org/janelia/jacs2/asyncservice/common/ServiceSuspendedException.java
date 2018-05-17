package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsServiceData;

/**
 * Exception thrown by a suspended service.
 */
public class ServiceSuspendedException extends RuntimeException {
    private JacsServiceData jacsServiceData;

    public ServiceSuspendedException(JacsServiceData jacsServiceData) {
        this.jacsServiceData = jacsServiceData;
    }

}

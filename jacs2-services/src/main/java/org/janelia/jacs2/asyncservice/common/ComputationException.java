package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsServiceData;

/**
 * Exception thrown by a service if something goes wrong during computation.
 */
public class ComputationException extends RuntimeException {
    private JacsServiceData jacsServiceData;

    public ComputationException(JacsServiceData jacsServiceData, String message) {
        super(message);
        this.jacsServiceData = jacsServiceData;
    }

    public ComputationException(JacsServiceData jacsServiceData, Throwable cause) {
        super(cause);
        this.jacsServiceData = jacsServiceData;
    }

}

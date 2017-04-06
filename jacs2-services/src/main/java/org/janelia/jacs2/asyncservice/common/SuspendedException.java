package org.janelia.jacs2.asyncservice.common;

import org.apache.commons.javaflow.api.Continuation;

/**
 * Exception thrown when a service was suspended.
 */
public class SuspendedException extends RuntimeException {

    private Continuation cont;
    private ContinuationCond contCond;

    public SuspendedException() {
    }

    public SuspendedException(Continuation cont, ContinuationCond contCond) {
        this.cont = cont;
        this.contCond = contCond;
    }

    public SuspendedException(Throwable cause) {
        super(cause);
    }

    public Continuation getCont() {
        return cont;
    }

    public ContinuationCond getContCond() {
        return contCond;
    }
}

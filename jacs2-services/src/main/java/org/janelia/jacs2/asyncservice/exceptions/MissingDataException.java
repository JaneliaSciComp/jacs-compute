package org.janelia.jacs2.asyncservice.exceptions;

import org.janelia.jacs2.asyncservice.common.ComputationException;

public class MissingDataException extends ComputationException {

    public MissingDataException(String msg) {
        super(msg);
    }
    public MissingDataException(String msg, Throwable e) {
        super(msg, e);
    }

}

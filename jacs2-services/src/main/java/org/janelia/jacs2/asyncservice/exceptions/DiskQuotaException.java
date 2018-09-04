package org.janelia.jacs2.asyncservice.exceptions;

import org.janelia.jacs2.asyncservice.common.ComputationException;

/**
 * User has exceeded disk quota.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class DiskQuotaException extends ComputationException {
    
    public DiskQuotaException(String message) {
        super(message);
    }
}

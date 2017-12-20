package org.janelia.jacs2.asyncservice.common;

/**
 * Exception thrown when a condition times out.
 */
public class CondTimeoutException extends RuntimeException {

    public CondTimeoutException() {
        super("Operation timed out");
    }

    public CondTimeoutException(long timeoutMs) {
        super("Operation timed out (elapsed time exceeded "+timeoutMs+"ms)");
    }
}

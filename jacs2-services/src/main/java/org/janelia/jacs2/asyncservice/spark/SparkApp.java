package org.janelia.jacs2.asyncservice.spark;

public interface SparkApp {
    String getAppId();
    boolean isDone();
    String getStatus();
    String getErrors();
    void kill();

    default boolean hasErrors() {
        String errors = getErrors();
        return errors != null && errors.trim().length() > 0;
    }
}

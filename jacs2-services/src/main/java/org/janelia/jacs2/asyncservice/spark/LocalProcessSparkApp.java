package org.janelia.jacs2.asyncservice.spark;

import org.apache.spark.launcher.SparkAppHandle;

public class LocalProcessSparkApp extends AbstractSparkApp {
    private final SparkAppHandle appHandle;

    public LocalProcessSparkApp(SparkAppHandle appHandle, String errorFilename) {
        super(errorFilename);
        this.appHandle = appHandle;
    }

    @Override
    public String getAppId() {
        return appHandle != null ? appHandle.getAppId() : null;
    }

    @Override
    public boolean isDone() {
        return appHandle != null && appHandle.getState().isFinal();
    }

    @Override
    public String getStatus() {
        return appHandle == null ? "NOT_FOUND" : appHandle.getState().name();
    }

    @Override
    public String getErrors() {
        if (appHandle.getState() == SparkAppHandle.State.KILLED) {
            return "Spark Application was terminated by the user";
        } else {
            return super.getErrors();
        }
    }

    @Override
    public void kill() {
        if (appHandle != null) {
            appHandle.kill();
        }
    }
}

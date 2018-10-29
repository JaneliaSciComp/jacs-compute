package org.janelia.jacs2.asyncservice.spark;

import org.apache.spark.launcher.SparkAppHandle;

public class SparkApp {

    private final SparkCluster cluster;
    private final String outputDir;
    private final String errorDir;
    private volatile SparkAppHandle handle;

    SparkApp(SparkCluster cluster, String outputDir, String errorDir) {
        this.cluster = cluster;
        this.outputDir = outputDir;
        this.errorDir = errorDir;
    }

    public SparkCluster getCluster() {
        return cluster;
    }

    public void updateHandle(SparkAppHandle handle) {
        if (this.handle == null) this.handle = handle;
    }

    public String getAppId() {
        return handle != null ? handle.getAppId() : null;
    }

    public boolean isDone() {
        return handle != null && handle.getState().isFinal();
    }

    public boolean isError() {
        return isDone() && handle.getState() != SparkAppHandle.State.FINISHED;
    }

    public void kill() {
        if (handle != null) {
            handle.kill();
        }
    }
}

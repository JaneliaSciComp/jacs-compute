package org.janelia.jacs2.asyncservice.common.spark;

import org.apache.spark.launcher.SparkAppHandle;

public class SparkApp {

    private SparkCluster cluster;
    private SparkAppHandle handle;

    public SparkApp(SparkCluster cluster, SparkAppHandle handle) {
        this.cluster = cluster;
        this.handle = handle;
    }

    public SparkCluster getCluster() {
        return cluster;
    }

    public SparkAppHandle getHandle() {
        return handle;
    }

    public boolean isDone() {
        return handle.getState().isFinal();
    }

    public boolean isError() {
        return isDone() && handle.getState() != SparkAppHandle.State.FINISHED;
    }
}
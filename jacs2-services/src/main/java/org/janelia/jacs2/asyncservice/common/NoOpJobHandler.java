package org.janelia.jacs2.asyncservice.common;

import java.util.Collection;
import java.util.Collections;

import org.janelia.model.service.JacsJobInstanceInfo;

class NoOpJobHandler implements ExeJobHandler {

    private final String jobInfo;

    NoOpJobHandler(String jobInfo) {
        this.jobInfo = jobInfo;
    }

    @Override
    public String getJobInfo() {
        return jobInfo;
    }

    @Override
    public boolean start() {
        return true;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public boolean hasFailed() {
        return false;
    }

    @Override
    public void terminate() {
        // nothing to do
    }

    @Override
    public Collection<JacsJobInstanceInfo> getJobInstances() {
        return Collections.emptyList();
    }
}

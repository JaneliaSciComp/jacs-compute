package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsJobInstanceInfo;

import java.util.Collection;

public interface ExeJobHandler {
    String getJobInfo();
    boolean start();
    boolean isDone();
    boolean hasFailed();
    void terminate();
    Collection<JacsJobInstanceInfo> getJobInstances();
}

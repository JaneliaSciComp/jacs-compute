package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsJobInstanceInfo;

import java.util.Collection;

public interface ExeJobInfo {
    String getScriptName();
    boolean isDone();
    boolean hasFailed();
    void terminate();
    Collection<JacsJobInstanceInfo> getJobInstanceInfos();
}

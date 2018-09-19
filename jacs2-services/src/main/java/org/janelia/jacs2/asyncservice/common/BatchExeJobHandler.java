package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsJobInstanceInfo;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class BatchExeJobHandler implements ExeJobHandler {
    private final String jobInfo;
    private final List<ExeJobHandler> jobBatch;

    BatchExeJobHandler(String jobInfo, List<ExeJobHandler> jobBatch) {
        this.jobInfo = jobInfo;
        this.jobBatch = jobBatch;
    }

    @Override
    public String getJobInfo() {
        return jobInfo;
    }

    @Override
    public boolean start() {
        return jobBatch.stream().map(ExeJobHandler::start).reduce(true, (s1, s2) -> s1 && s2);
    }

    @Override
    public boolean isDone() {
        // is done if all individual jobs are done
        return jobBatch.stream().map(j -> j.isDone()).reduce(true, (f1, f2) -> f1 && f2);
    }

    @Override
    public boolean hasFailed() {
        // has failed if any of the individual jobs has failed
        return jobBatch.stream().map(j -> j.hasFailed()).reduce(false, (f1, f2) -> f1 || f2);
    }

    @Override
    public void terminate() {
        jobBatch.forEach(j -> j.terminate());
    }

    @Override
    public Collection<JacsJobInstanceInfo> getJobInstances() {
        return jobBatch.stream().flatMap(j -> j.getJobInstances().stream()).collect(Collectors.toList());
    }
}

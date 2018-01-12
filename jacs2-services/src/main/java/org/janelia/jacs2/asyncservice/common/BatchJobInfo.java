package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsJobInstanceInfo;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class BatchJobInfo implements ExeJobInfo {
    private final String scriptName;
    private final List<ExeJobInfo> jobBatch;

    public BatchJobInfo(List<ExeJobInfo> jobBatch, String scriptName) {
        this.jobBatch = jobBatch;
        this.scriptName = scriptName;
    }

    @Override
    public String getScriptName() {
        return scriptName;
    }

    @Override
    public String start() {
        return jobBatch
                .stream()
                .map(ExeJobInfo::start)
                .filter(Objects::nonNull)
                .reduce((j1, j2) -> j1 + "," + j2)
                .orElse(null)
                ;
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
    public Collection<JacsJobInstanceInfo> getJobInstanceInfos() {
        return jobBatch.stream().flatMap(j -> j.getJobInstanceInfos().stream()).collect(Collectors.toList());
    }
}

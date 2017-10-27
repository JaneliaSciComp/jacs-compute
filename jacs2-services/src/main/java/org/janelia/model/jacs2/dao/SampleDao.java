package org.janelia.model.jacs2.dao;

import org.janelia.model.jacs2.domain.Subject;
import org.janelia.model.jacs2.domain.sample.PipelineResult;
import org.janelia.model.jacs2.domain.sample.Sample;
import org.janelia.model.jacs2.domain.sample.SamplePipelineRun;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;

import java.util.Date;

public interface SampleDao extends DomainObjectDao<Sample> {
    PageResult<Sample> findMatchingSamples(Subject subject, Sample pattern, DataInterval<Date> tmogInterval, PageRequest pageRequest);
    void addObjectivePipelineRun(Sample sample, String objective, SamplePipelineRun samplePipelineRun);
    void addSampleObjectivePipelineRunResult(Sample sample, String objective, Number runId, Number parentResultId, PipelineResult pipelineResult);
    void updateSampleObjectivePipelineRunResult(Sample sample, String objective, Number runId, PipelineResult pipelineResult);
}

package org.janelia.jacs2.dao;

import org.janelia.it.jacs.model.domain.Subject;
import org.janelia.it.jacs.model.domain.sample.PipelineResult;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.it.jacs.model.domain.sample.SamplePipelineRun;
import org.janelia.jacs2.model.DataInterval;
import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.page.PageResult;

import java.util.Date;

public interface SampleDao extends DomainObjectDao<Sample> {
    PageResult<Sample> findMatchingSamples(Subject subject, Sample pattern, DataInterval<Date> tmogInterval, PageRequest pageRequest);
    void addObjectivePipelineRun(Sample sample, String objective, SamplePipelineRun samplePipelineRun);
    void addSampleObjectivePipelineRunResult(Sample sample, String objective, Number runId, Number parentResultId, PipelineResult pipelineResult);
}

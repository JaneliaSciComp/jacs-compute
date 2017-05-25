package org.janelia.jacs2.dao;

import org.janelia.it.jacs.model.domain.Subject;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.it.jacs.model.domain.sample.SamplePipelineRun;
import org.janelia.jacs2.model.DataInterval;
import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.page.PageResult;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

public interface SampleDao extends DomainObjectDao<Sample> {
    PageResult<Sample> findMatchingSamples(Subject subject, Sample pattern, DataInterval<Date> tmogInterval, PageRequest pageRequest);
    void addObjectivePipelineResults(Sample sample, Map<String, Collection<SamplePipelineRun>> sampleRuns);
}

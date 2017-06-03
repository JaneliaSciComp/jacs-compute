package org.janelia.it.jacs.model.domain.sample;

import com.google.common.collect.ImmutableList;
import org.janelia.it.jacs.model.domain.IndexedReference;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class SamplePipelineRunTest {

    @SuppressWarnings("unchecked")
    @Test
    public void traverseResults() {
        SamplePipelineRun pr = new SamplePipelineRun();
        PipelineResult r0 = addPipelineResult("r0", pr.getResults());
        PipelineResult r1 = addPipelineResult("r1", pr.getResults());
        PipelineResult r2 = addPipelineResult("r2", pr.getResults());

        PipelineResult r0_0 = addPipelineResult("r0_0", r0.getResults());
        PipelineResult r0_1 = addPipelineResult("r0_1", r0.getResults());
        PipelineResult r0_2 = addPipelineResult("r0_2", r0.getResults());

        PipelineResult r1_0 = addPipelineResult("r1_0", r1.getResults());
        PipelineResult r1_1 = addPipelineResult("r1_1", r1.getResults());
        PipelineResult r1_2 = addPipelineResult("r1_2", r1.getResults());

        PipelineResult r2_0 = addPipelineResult("r2_0", r2.getResults());
        PipelineResult r2_1 = addPipelineResult("r2_1", r2.getResults());
        PipelineResult r2_2 = addPipelineResult("r2_2", r2.getResults());

        PipelineResult r0_0_0 = addPipelineResult("r0_0_0", r0_0.getResults());

        PipelineResult r0_1_0 = addPipelineResult("r0_1_0", r0_1.getResults());

        PipelineResult r0_2_0 = addPipelineResult("r0_2_0", r0_2.getResults());

        PipelineResult r1_0_0 = addPipelineResult("r1_0_0", r1_0.getResults());

        PipelineResult r1_1_0 = addPipelineResult("r1_1_0", r1_1.getResults());

        PipelineResult r1_2_0 = addPipelineResult("r1_2_0", r1_2.getResults());

        PipelineResult r2_2_0 = addPipelineResult("r2_2_0", r2_2.getResults());

        ImmutableList.Builder<IndexedReference<PipelineResult, IndexedReference<Integer, Integer>[]>> expectedResultBuilder = ImmutableList.builder();
        expectedResultBuilder
                .add(new IndexedReference<>(r0, new IndexedReference[]{
                        new IndexedReference<>(0, 0)
                }))
                .add(new IndexedReference<>(r0_0, new IndexedReference[]{
                        new IndexedReference<>(0, 0),
                        new IndexedReference<>(1, 0)
                }))
                .add(new IndexedReference<>(r0_0_0, new IndexedReference[]{
                        new IndexedReference<>(0, 0),
                        new IndexedReference<>(1, 0),
                        new IndexedReference<>(2, 0)
                }))
                .add(new IndexedReference<>(r0_1, new IndexedReference[]{
                        new IndexedReference<>(0, 0),
                        new IndexedReference<>(1, 1)
                }))
                .add(new IndexedReference<>(r0_1_0, new IndexedReference[]{
                        new IndexedReference<>(0, 0),
                        new IndexedReference<>(1, 1),
                        new IndexedReference<>(2, 0)
                }))
                .add(new IndexedReference<>(r0_2, new IndexedReference[]{
                        new IndexedReference<>(0, 0),
                        new IndexedReference<>(1, 2)
                }))
                .add(new IndexedReference<>(r0_2_0, new IndexedReference[]{
                        new IndexedReference<>(0, 0),
                        new IndexedReference<>(1, 2),
                        new IndexedReference<>(2, 0)
                }))
                .add(new IndexedReference<>(r1, new IndexedReference[]{
                        new IndexedReference<>(0, 1)
                }))
                .add(new IndexedReference<>(r1_0, new IndexedReference[]{
                        new IndexedReference<>(0, 1),
                        new IndexedReference<>(1, 0)
                }))
                .add(new IndexedReference<>(r1_0_0, new IndexedReference[]{
                        new IndexedReference<>(0, 1),
                        new IndexedReference<>(1, 0),
                        new IndexedReference<>(2, 0)
                }))
                .add(new IndexedReference<>(r1_1, new IndexedReference[]{
                        new IndexedReference<>(0, 1),
                        new IndexedReference<>(1, 1)
                }))
                .add(new IndexedReference<>(r1_1_0, new IndexedReference[]{
                        new IndexedReference<>(0, 1),
                        new IndexedReference<>(1, 1),
                        new IndexedReference<>(2, 0)
                }))
                .add(new IndexedReference<>(r1_2, new IndexedReference[]{
                        new IndexedReference<>(0, 1),
                        new IndexedReference<>(1, 2)
                }))
                .add(new IndexedReference<>(r1_2_0, new IndexedReference[]{
                        new IndexedReference<>(0, 1),
                        new IndexedReference<>(1, 2),
                        new IndexedReference<>(2, 0)
                }))
                .add(new IndexedReference<>(r2, new IndexedReference[]{
                        new IndexedReference<>(0, 2)
                }))
                .add(new IndexedReference<>(r2_0, new IndexedReference[]{
                        new IndexedReference<>(0, 2),
                        new IndexedReference<>(1, 0)
                }))
                .add(new IndexedReference<>(r2_1, new IndexedReference[]{
                        new IndexedReference<>(0, 2),
                        new IndexedReference<>(1, 1)
                }))
                .add(new IndexedReference<>(r2_2, new IndexedReference[]{
                        new IndexedReference<>(0, 2),
                        new IndexedReference<>(1, 2)
                }))
                .add(new IndexedReference<>(r2_2_0, new IndexedReference[]{
                        new IndexedReference<>(0, 2),
                        new IndexedReference<>(1, 2),
                        new IndexedReference<>(2, 0)
                }))
        ;
        assertThat(pr.streamResults().collect(Collectors.toList()), equalTo(expectedResultBuilder.build()));
    }

    private PipelineResult addPipelineResult(String resultName, List<PipelineResult> results) {
        PipelineResult r = new PipelineResult();
        r.setName(resultName);
        results.add(r);
        return r;
    }
}

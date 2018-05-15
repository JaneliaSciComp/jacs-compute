package org.janelia.jacs2.asyncservice.workflow;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.janelia.dagobah.DAG;
import org.janelia.dagobah.Task;
import org.janelia.dagobah.TaskStatus;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.enums.algorithms.AlignmentAlgorithm;
import org.janelia.model.domain.enums.algorithms.MergeAlgorithm;
import org.janelia.model.domain.enums.algorithms.StitchAlgorithm;
import org.janelia.model.domain.sample.LSMImage;
import org.janelia.model.domain.sample.ObjectiveSample;
import org.janelia.model.domain.sample.Sample;

import java.io.File;
import java.util.*;

/**
 * The Sample Workflow is a Dagobah-based DAG workflow with many capabilities:
 * LSM metadata extraction
 * Distortion correction
 * Merging
 * Stitching
 * Alignments
 * Neuron Separation
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SampleWorkflow {

    private SamplePipelineConfiguration config;
    private Set<SamplePipelineStep> force;

    public SampleWorkflow(SamplePipelineConfiguration config, Set<SamplePipelineStep> force) {
        this.config = config;
        this.force = force;
    }

    public enum SamplePipelineStep {
        LSMProcessing,
        SampleProcessing,
        PostProcessing,
        Alignment
    }

    public class SamplePipelineConfiguration {
        private String name;
        private String objective;
        private boolean normalization;
        private boolean distortionCorrection;
        private MergeAlgorithm mergeAlgorithm;
        private StitchAlgorithm stitchAlgorithm;
        private String channelDyeSpec;
        private String outputChannelOrder;
        private String outputColorSpec;
        private String imageType;
        private List<AlignmentConfiguration> alignments;
        private boolean neuronSeparation;
    }

    public class AlignmentConfiguration {
        private AlignmentAlgorithm algorithm;
        private String algorithmParam;
        private String algorithmResultName;
    }

    private void createPipeline(Sample sample, List<LSMImage> lsms) {

        DAG dag = new DAG<Task>();

        for (ObjectiveSample objectiveSample : sample.getObjectiveSamples()) {

            Task update = createLSMSummaryUpdateTask(objectiveSample);

            Multimap<String, Task> lsmTaskByAreaTile = ArrayListMultimap.create();
            for(LSMImage lsm : lsms) {
                if (lsm.getObjective().equals(objectiveSample.getObjective())) {
                    // Copy LSM, extract metadata, generate artifacts, apply distortion correction
                    Task lsmSummary = createLSMProcessingTask(lsm);
                    lsmTaskByAreaTile.put(lsm.getAnatomicalArea() + "~" + lsm.getTile(), lsmSummary);

                    // Update database
                    dag.addEdge(lsmSummary, update);
                }
            }

            Task postUpdate = createPostProcessingUpdateTask(objectiveSample);

            Multimap<String, Task> normalizeTaskByArea = ArrayListMultimap.create();
            for (String areaTile : lsmTaskByAreaTile.keys()) {

                String[] key = areaTile.split("~");
                String area = key[0];
                String tile = key[1];

                Collection<Task> lsmTasks = lsmTaskByAreaTile.get(areaTile);

                if (lsmTasks.isEmpty()) {
                    throw new IllegalStateException("No task(s) found for tile="+tile);
                }
                else if (lsmTasks.size()>1) {
                    Task merge = createMergeTask(objectiveSample, tile);
                    Task post = createPostProcessingTask(objectiveSample, tile);
                    Task normalize = createNormalizationTask(objectiveSample, tile);

                    dag.addEdges(lsmTasks, merge); // lsm processing -> merge
                    dag.addEdge(merge, post); // merge -> post processing
                    dag.addEdge(post, postUpdate); // post processing -> update sample
                    dag.addEdge(merge, normalize); // merge -> normalization

                    normalizeTaskByArea.put(area, normalize);
                }
                else {
                    // skip merge because there is only one image in this tile

                    Task lsmTask = lsmTasks.iterator().next();
                    Task normalize = createNormalizationTask(objectiveSample, tile);
                    dag.addEdge(lsmTask, normalize); // lsm processing -> normalization

                    normalizeTaskByArea.put(area, normalize);
                }
            }

            for (String area : normalizeTaskByArea.keys()) {

                Collection<Task> normalizeTasks = normalizeTaskByArea.get(area);
                Task stitch;

                if (normalizeTasks.isEmpty()) {
                    throw new IllegalStateException("No task(s) found for area="+area);
                }
                else if (normalizeTasks.size()>1) {
                    Task group = createGroupTask(objectiveSample, area);
                    stitch = createStitchTask(objectiveSample, area);

                    dag.addEdges(normalizeTasks, group); // normalize -> group
                    dag.addEdge(group, stitch); // group -> stitch
                }
                else {
                    // skip group/stitch because there is only one tile
                    stitch = normalizeTasks.iterator().next();
                }

                Task artifactUpdate = createArtifactUpdateTask(objectiveSample);

                Task artifacts = createArtifactTask(objectiveSample, area);
                dag.addEdge(stitch, artifacts); // stitch -> artifacts
                dag.addEdge(artifacts, artifactUpdate); // post processing -> update sample

                Task post = createPostProcessingTask(objectiveSample, area);
                dag.addEdge(stitch, post); // stitch -> post processing
                dag.addEdge(post, postUpdate); // post processing -> update sample


                // TODO: add alignments
                // TODO: add neuron separation

            }
        }
    }

    private Task createLSMProcessingTask(LSMImage lsm) {
        return null;
    }

    private Task createDistortionCorrectionTask(LSMImage lsm) {
        return null;
    }

    private Task createLSMSummaryUpdateTask(ObjectiveSample objectiveSample) {
        return null;
    }

    private Task createMergeTask(ObjectiveSample objectiveSample, String tileName) {
        return null;
    }

    private Task createNormalizationTask(ObjectiveSample objectiveSample, String tileName) {
        return null;
    }

    private Task createGroupTask(ObjectiveSample objectiveSample, String areaName) {
        return null;
    }

    private Task createPostProcessingTask(ObjectiveSample objectiveSample, String tileName) {
        return null;
    }

    private Task createPostProcessingUpdateTask(ObjectiveSample objectiveSample) {
        return null;
    }

    private Task createStitchTask(ObjectiveSample objectiveSample, String tileName) {
        return null;
    }

    private Task createArtifactTask(ObjectiveSample objectiveSample, String tileName) {
        return null;
    }

    private Task createArtifactUpdateTask(ObjectiveSample objectiveSample) {
        return null;
    }

    private Long getNewId() {
        // TODO:
        return 0L;
    }


    private Map<String, Object> map(Object... values) {
        Map<String, Object> map = new HashMap<>();
        for (int i=0; i<values.length; i+=2) {
            String key = (String)values[i];
            Object value = values[i+1];
            map.put(key, value);
        }
        return map;
    }




}

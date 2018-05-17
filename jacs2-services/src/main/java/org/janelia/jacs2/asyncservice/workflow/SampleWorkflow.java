package org.janelia.jacs2.asyncservice.workflow;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.janelia.dagobah.DAG;
import org.janelia.jacs2.asyncservice.sample.CopyLSMService;
import org.janelia.jacs2.asyncservice.sample.DistortionCorrectionService;
import org.janelia.jacs2.asyncservice.sample.LSMProcessingService;
import org.janelia.jacs2.asyncservice.sample.LSMSummaryUpdateService;
import org.janelia.model.access.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.model.domain.sample.LSMImage;
import org.janelia.model.domain.sample.ObjectiveSample;
import org.janelia.model.domain.sample.Sample;
import org.janelia.model.domain.workflow.SamplePipelineConfiguration;
import org.janelia.model.domain.workflow.SamplePipelineOutput;
import org.janelia.model.domain.workflow.WorkflowTask;

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

    private final TimebasedIdentifierGenerator idGenerator;
    private final SamplePipelineConfiguration config;
    private final Set<SamplePipelineOutput> force;

    public SampleWorkflow(SamplePipelineConfiguration config, Set<SamplePipelineOutput> force) {
        this.idGenerator = new TimebasedIdentifierGenerator(0);
        this.config = config;
        this.force = force;
    }

    /**
     * Create a pipeline workflow for the given sample.
     * @param sample the sample to process
     * @param lsms the sample's LSM images
     * @return
     */
    public DAG<WorkflowTask> createPipeline(Sample sample, List<LSMImage> lsms) {

        DAG<WorkflowTask> dag = new DAG();

        // Treat each objective separately
        for (ObjectiveSample objectiveSample : sample.getObjectiveSamples()) {

            // Copy the LSMs and run distortion correction
            Multimap<String, WorkflowTask> lsmTaskByAreaTile = ArrayListMultimap.create();
            Collection<WorkflowTask> lsmCopyTasks = new ArrayList<>();
            for(LSMImage lsm : lsms) {
                if (lsm.getObjective().equals(objectiveSample.getObjective())) {

                    WorkflowTask copy = createCopyLSMTask(lsm);
                    lsmCopyTasks.add(copy);

                    WorkflowTask lsmTask = copy;
                    if (config.isDistortionCorrection()) {
                        WorkflowTask dist = createDistortionCorrectionTask(lsm);
                        dag.addEdge(copy, dist);
                        lsmTask = dist;
                    }

                    String areaTile = lsm.getAnatomicalArea() + "~" + lsm.getTile();
                    lsmTaskByAreaTile.put(areaTile, lsmTask);
                }
            }

            // Run "LSM Summary"
            WorkflowTask lsmSummary = createLSMProcessingTask(objectiveSample);
            WorkflowTask update = createLSMSummaryUpdateTask(objectiveSample);

            dag.addEdges(lsmCopyTasks, lsmSummary);
            dag.addEdge(lsmSummary, update);

//            // For each tile, merge and normalize the LSMs
//            Multimap<String, WorkflowTask> normalizeTaskByArea = ArrayListMultimap.create();
//            for (String areaTile : lsmTaskByAreaTile.keySet()) {
//
//                String[] key = areaTile.split("~");
//                String area = key[0];
//                String tile = key[1];
//
//                Collection<WorkflowTask> lsmTasks = lsmTaskByAreaTile.get(areaTile);
//
//                if (lsmTasks.isEmpty()) {
//                    throw new IllegalStateException("No task(s) found for tile="+tile);
//                }
//                else if (lsmTasks.size()>1) {
//                    WorkflowTask merge = createMergeTask(objectiveSample, tile);
//                    WorkflowTask normalize = createNormalizationTask(objectiveSample, tile);
//
//                    dag.addEdges(lsmTasks, merge); // lsm processing -> merge
//                    dag.addEdge(merge, normalize); // merge -> normalization
//
//                    normalizeTaskByArea.put(area, normalize);
//                }
//                else {
//                    // skip merge because there is only one image in this tile
//
//                    WorkflowTask lsmTask = lsmTasks.iterator().next();
//                    WorkflowTask normalize = createNormalizationTask(objectiveSample, tile);
//                    dag.addEdge(lsmTask, normalize); // lsm processing -> normalization
//
//                    normalizeTaskByArea.put(area, normalize);
//                }
//            }
//
//            // For each area, stitch the tiles
//            for (String area : normalizeTaskByArea.keySet()) {
//
//                Collection<WorkflowTask> normalizeTasks = normalizeTaskByArea.get(area);
//                WorkflowTask stitch;
//
//                if (normalizeTasks.isEmpty()) {
//                    throw new IllegalStateException("No task(s) found for area="+area);
//                }
//                else if (normalizeTasks.size()>1) {
//                    WorkflowTask group = createGroupTask(objectiveSample, area);
//                    stitch = createStitchTask(objectiveSample, area);
//
//                    dag.addEdges(normalizeTasks, group); // normalize -> group
//                    dag.addEdge(group, stitch); // group -> stitch
//                }
//                else {
//                    // skip group/stitch because there is only one tile
//                    stitch = normalizeTasks.iterator().next();
//                }
//
//                WorkflowTask sampleProcessingUpdate = createSampleProcessingUpdateTask(objectiveSample, area);
//
//                WorkflowTask sampleProcessing = createSampleProcessingTask(objectiveSample, area);
//                dag.addEdge(stitch, sampleProcessing); // stitch -> sample processing
//                dag.addEdge(sampleProcessing, sampleProcessingUpdate); // sample processing -> update sample
//
//                WorkflowTask post = createPostProcessingTask(objectiveSample, area);
//                dag.addEdge(stitch, post); // stitch -> post processing
//
//                WorkflowTask postUpdate = createPostProcessingUpdateTask(objectiveSample);
//                dag.addEdges(normalizeTasks, postUpdate); // normalize -> update sample
//                dag.addEdge(post, postUpdate); // post processing -> update sample
//
//                // TODO: add alignments
//                // TODO: add neuron separation
//
//            }
        }

        return dag;
    }

    private WorkflowTask createTask() {
        WorkflowTask task = new WorkflowTask();
        task.setId(getNewId());
        return task;
    }

    private WorkflowTask createCopyLSMTask(LSMImage lsm) {
        WorkflowTask task = createTask();
        task.setName("Copy ("+lsm.getObjective()+"/"+lsm.getAnatomicalArea()+"/"+lsm.getTile()+")");
        task.setServiceClass(CopyLSMService.class.getName());
        return task;
    }

    private WorkflowTask createLSMProcessingTask(ObjectiveSample objectiveSample) {
        WorkflowTask task = createTask();
        task.setName("LSM Summary ("+objectiveSample.getObjective()+")");
        task.setServiceClass(LSMProcessingService.class.getName());
        return task;
    }

    private WorkflowTask createDistortionCorrectionTask(LSMImage lsm) {
        WorkflowTask task = createTask();
        task.setName("Distortion Correction ("+lsm.getObjective()+")");
        task.setServiceClass(DistortionCorrectionService.class.getName());
        return task;
    }

    private WorkflowTask createLSMSummaryUpdateTask(ObjectiveSample objectiveSample) {
        WorkflowTask task = createTask();
        task.setName("LSM Summary Update ("+objectiveSample.getObjective()+")");
        task.setServiceClass(LSMSummaryUpdateService.class.getName());
        task.setHasEffects(true);
        return task;
    }

    private WorkflowTask createMergeTask(ObjectiveSample objectiveSample, String tileName) {
        WorkflowTask task = createTask();
        task.setName("Merge ("+tileName+")");
        Map<String, Object> inputs = task.getInputs();
        inputs.put("mergeAlgorithm", config.getMergeAlgorithm());
        return task;
    }

    private WorkflowTask createNormalizationTask(ObjectiveSample objectiveSample, String tileName) {
        WorkflowTask task = createTask();
        task.setName("Normalization ("+tileName+")");
        Map<String, Object> inputs = task.getInputs();
        inputs.put("channelDyeSpec", config.getChannelDyeSpec());
        inputs.put("outputChannelOrder", config.getOutputChannelOrder());
        inputs.put("outputColorSpec", config.getOutputColorSpec());
        return task;
    }

    private WorkflowTask createGroupTask(ObjectiveSample objectiveSample, String areaName) {
        WorkflowTask task = createTask();
        task.setName("Group ("+areaName+")");
        return task;
    }

    private WorkflowTask createPostProcessingTask(ObjectiveSample objectiveSample, String tileName) {
        WorkflowTask task = createTask();
        task.setName("Post Processing ("+tileName+")");
        return task;
    }

    private WorkflowTask createPostProcessingUpdateTask(ObjectiveSample objectiveSample) {
        WorkflowTask task = createTask();
        task.setName("Post Processing Update ("+objectiveSample.getObjective()+")");
        task.setHasEffects(true);
        Map<String, Object> inputs = task.getInputs();
        inputs.put("postAlgorithm", config.getPostAlgorithm());
        return task;
    }

    private WorkflowTask createStitchTask(ObjectiveSample objectiveSample, String tileName) {
        WorkflowTask task = createTask();
        task.setName("Stitch ("+objectiveSample.getObjective()+"/"+tileName+")");
        return task;
    }

    private WorkflowTask createSampleProcessingTask(ObjectiveSample objectiveSample, String tileName) {
        WorkflowTask task = createTask();
        task.setName("Sample Processing ("+objectiveSample.getObjective()+"/"+tileName+")");
        return task;
    }

    private WorkflowTask createSampleProcessingUpdateTask(ObjectiveSample objectiveSample, String area) {
        WorkflowTask task = createTask();
        task.setName("Sample Processing Update ("+area+")");
        task.setHasEffects(true);
        return task;
    }

    private Long getNewId() {
        return idGenerator.generateId().longValue();
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

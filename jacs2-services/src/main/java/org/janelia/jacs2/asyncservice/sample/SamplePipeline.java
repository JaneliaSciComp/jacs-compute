package org.janelia.jacs2.asyncservice.sample;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.ValueGraphBuilder;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.interfaces.HasIdentifier;
import org.janelia.model.domain.sample.LSMImage;
import org.janelia.model.domain.sample.ObjectiveSample;
import org.janelia.model.domain.sample.Sample;

import java.io.File;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SamplePipeline {

    public enum SamplePipelineStep {
        LSMProcessing,
        SampleProcessing,
        PostProcessing
    }

    public class SamplePipelineConfiguration {




    }

    private void createPipeline(Set<SamplePipelineStep> reuse) {

        DAG dag = new DAG();

        Sample s = new Sample();

        for (ObjectiveSample objectiveSample : s.getObjectiveSamples()) {

            List<LSMImage> lsms = new ArrayList<>();

            Multimap<String, Task> lsmTaskByAreaTile = ArrayListMultimap.create();
            for(LSMImage lsm : lsms) {
                Task task = createLSMTask(lsm); // Copy LSM, extract metadata, generate artifacts, apply distortion correction
                lsmTaskByAreaTile.put(lsm.getAnatomicalArea()+"~"+lsm.getTile(), task);
                dag.addTask(task);
            }

            Task postUpdate = createPostProcessingUpdateTask();
            dag.addTask(postUpdate);

            Multimap<String, Task> mergeTaskByArea = ArrayListMultimap.create();
            for (String areaTile : lsmTaskByAreaTile.keys()) {

                String[] key = areaTile.split("~");
                String area = key[0];
                String tile = key[1];

                Collection<Task> tasks = lsmTaskByAreaTile.get(areaTile);

                if (tasks.isEmpty()) {
                    throw new IllegalStateException("No task(s) found for tile="+tile);
                }
                else if (tasks.size()>1) {
                    Task merge = createMergeTask(tile);
                    dag.addTask(merge).addEdges(tasks, merge); // lsm processing -> merge

                    Task post = createPostProcessingTask(tile);
                    dag.addTask(post)
                            .addEdge(merge, post) // merge -> post processing
                            .addEdge(post, postUpdate); // post processing -> update sample

                    mergeTaskByArea.put(area, merge);
                }
                else {
                    // skip merge because there is only one image in this tile
                    Task task = tasks.iterator().next();
                    mergeTaskByArea.put(area, task);
                }
            }

            for (String area : mergeTaskByArea.keys()) {
                Collection<Task> tasks = mergeTaskByArea.get(area);

                Task stitch = null;

                if (tasks.isEmpty()) {
                    throw new IllegalStateException("No task(s) found for area="+area);
                }
                else if (tasks.size()>1) {
                    Task group = createGroupTask(area);
                    dag.addTask(group).addEdges(tasks, group); // merge -> group

                    stitch = createStitchTask(area);
                    dag.addTask(stitch).addEdge(group, stitch); // group -> stitch
                }
                else {
                    // skip group/stitch because there is only one tile
                    stitch = tasks.iterator().next();
                }

                Task artifactUpdate = createArtifactUpdateTask();
                dag.addTask(artifactUpdate);

                Task artifacts = createArtifactTask(area);
                dag.addTask(artifacts)
                    .addEdge(stitch, artifacts) // stitch -> artifacts
                    .addEdge(artifacts, artifactUpdate); // post processing -> update sample


                Task post = createPostProcessingTask(area);
                dag.addTask(post)
                    .addEdge(stitch, post) // stitch -> post processing
                    .addEdge(post, postUpdate); // post processing -> update sample


                // TODO: add alignments
                // TODO: add neuron separation

            }
        }
    }


    private Task createLSMTask(LSMImage lsm) {
        return null;
    }

    private Task createMergeTask(String tileName) {
        return null;
    }

    private Task createGroupTask(String areaName) {
        return null;
    }

    private Task createPostProcessingTask(String tileName) {
        return null;
    }

    private Task createPostProcessingUpdateTask() {
        return null;
    }

    private Task createStitchTask(String tileName) {
        return null;
    }

    private Task createArtifactTask(String tileName) {
        return null;
    }

    private Task createArtifactUpdateTask() {
        return null;
    }





    private String getArea(String tile) {
        return null;
    }


    class DAG {

        private final Map<Long,Task> tasks = new HashMap<>();
        private final Map<Long,Set<Long>> edges = new HashMap<>();

        public DAG addTask(Task task) {
            tasks.put(task.getId(), task);
            return this;
        }

        public DAG addEdge(Task sourceTask, Task targetTask) {
            Set<Long> adjacent = edges.get(sourceTask.getId());
            if (adjacent==null) {
                adjacent = new HashSet<>();
                edges.put(sourceTask.getId(), adjacent);
            }
            adjacent.add(targetTask.getId());
            return this;
        }

        public DAG addEdges(Task sourceTask, Collection<Task> targetTasks) {
            targetTasks.stream().forEach(t -> addEdge(sourceTask, t));
            return this;
        }

        public DAG addEdges(Collection<Task> sourceTasks, Task targetTask) {
            sourceTasks.stream().forEach(t -> addEdge(t, targetTask));
            return this;
        }

        public Set<Task> getDownstream(Task task) {
            return edges.get(task.getId()).stream().map(id -> tasks.get(id)).collect(Collectors.toSet());
        }

        public Set<Task> getUpstream(Task task) {
            Set<Task> upstream = new HashSet<>();
            for (Map.Entry<Long, Set<Long>> entry : edges.entrySet()) {
                Long sourceId = entry.getKey();
                for(Long targetId : entry.getValue()) {
                    if (targetId.equals(task.getId())) {
                        upstream.add(tasks.get(sourceId));
                    }
                }
            }
            return upstream;
        }
    }

    enum TaskStatus {
        Pending(false),
        Running(false),
        Complete(true),
        Error(true);
        private boolean isFinal;
        TaskStatus(boolean isFinal) {
            this.isFinal = isFinal;
        }
        public boolean isFinal() {
            return isFinal;
        }
    }

    class Task<U,V> implements HasIdentifier {

        private Long id;
        private String serviceClassName;
        private TaskStatus status;
        private boolean isVarIn = false;
        private boolean isVarOut = false;
        private List<U> inputs;
        private List<V> outputs;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getServiceClassName() {
            return serviceClassName;
        }

        public void setServiceClassName(String serviceClassName) {
            this.serviceClassName = serviceClassName;
        }

        public List<V> getOutputs() {
            return outputs;
        }

    }

    class TaskData {

    }

    class Image extends TaskData {
        File rawFile;
        File lossyFile;

    }

    class PrimaryImage extends Image {
        private Long lsmId;
        private File temporaryFile;
        private File correctedFile;
    }

    class SecondaryImage extends Image {
        private List<Image> inputs;
        private Map<FileType, File> mips;
        private Map<FileType, File> movies;
    }

    class TileImage extends SecondaryImage {
        private String tileName;
    }

    class AreaImage extends SecondaryImage {
        private String areaName;
    }

    class AlignedImage extends AreaImage {
        private String alignmentSpace;
    }



}

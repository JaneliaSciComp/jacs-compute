package org.janelia.jacs2.asyncservice.workflow;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.interfaces.HasIdentifier;
import org.janelia.model.domain.sample.LSMImage;
import org.janelia.model.domain.sample.ObjectiveSample;
import org.janelia.model.domain.sample.Sample;

import java.io.File;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SampleWorkflow {

    public enum SamplePipelineStep {
        LSMProcessing,
        SampleProcessing,
        PostProcessing,
        Alignment
    }

    public class SamplePipelineConfiguration {

        private String name;
        private String objective;
        private String mergeAlgorithm;
        private String channelDyeSpec;
        private String outputChannelOrder;
        private String outputColorSpec;
        private String imageType;
        private List<AlignmentConfiguration> alignments;
    }

    public class AlignmentConfiguration {
        private String algorithmName;
        private String algorithmParam;
        private String algorithmResultName;
    }

//    private void createPipeline(Sample sample, Set<SamplePipelineStep> reuse) {
//
//        DAG dag = new DAG();
//
//        for (ObjectiveSample objectiveSample : sample.getObjectiveSamples()) {
//
//            List<LSMImage> lsms = new ArrayList<>(); // TODO: get the LSMs for the sample
//
//            Multimap<String, Task> lsmTaskByAreaTile = ArrayListMultimap.create();
//            for(LSMImage lsm : lsms) {
//                Task task = createLSMTask(lsm); // Copy LSM, extract metadata, generate artifacts, apply distortion correction
//                lsmTaskByAreaTile.put(lsm.getAnatomicalArea()+"~"+lsm.getTile(), task);
//                dag.addTask(task);
//            }
//
//            Task postUpdate = createPostProcessingUpdateTask();
//            dag.addTask(postUpdate);
//
//            Multimap<String, Task> mergeTaskByArea = ArrayListMultimap.create();
//            for (String areaTile : lsmTaskByAreaTile.keys()) {
//
//                String[] key = areaTile.split("~");
//                String area = key[0];
//                String tile = key[1];
//
//                Collection<Task> tasks = lsmTaskByAreaTile.get(areaTile);
//
//                if (tasks.isEmpty()) {
//                    throw new IllegalStateException("No task(s) found for tile="+tile);
//                }
//                else if (tasks.size()>1) {
//                    Task merge = createMergeTask(tile);
//                    dag.addTask(merge).addEdges(tasks, merge); // lsm processing -> merge
//
//                    Task post = createPostProcessingTask(tile);
//                    dag.addTask(post)
//                            .addEdge(merge, post) // merge -> post processing
//                            .addEdge(post, postUpdate); // post processing -> update sample
//
//                    mergeTaskByArea.put(area, merge);
//                }
//                else {
//                    // skip merge because there is only one image in this tile
//                    Task task = tasks.iterator().next();
//                    mergeTaskByArea.put(area, task);
//                }
//            }
//
//            for (String area : mergeTaskByArea.keys()) {
//                Collection<Task> tasks = mergeTaskByArea.get(area);
//
//                Task stitch = null;
//
//                if (tasks.isEmpty()) {
//                    throw new IllegalStateException("No task(s) found for area="+area);
//                }
//                else if (tasks.size()>1) {
//                    Task group = createGroupTask(area);
//                    dag.addTask(group).addEdges(tasks, group); // merge -> group
//
//                    stitch = createStitchTask(area);
//                    dag.addTask(stitch).addEdge(group, stitch); // group -> stitch
//                }
//                else {
//                    // skip group/stitch because there is only one tile
//                    stitch = tasks.iterator().next();
//                }
//
//                Task artifactUpdate = createArtifactUpdateTask();
//                dag.addTask(artifactUpdate);
//
//                Task artifacts = createArtifactTask(area);
//                dag.addTask(artifacts)
//                    .addEdge(stitch, artifacts) // stitch -> artifacts
//                    .addEdge(artifacts, artifactUpdate); // post processing -> update sample
//
//
//                Task post = createPostProcessingTask(area);
//                dag.addTask(post)
//                    .addEdge(stitch, post) // stitch -> post processing
//                    .addEdge(post, postUpdate); // post processing -> update sample
//
//
//                // TODO: add alignments
//                // TODO: add neuron separation
//
//            }
//        }
//    }
//
//
//    private Task createLSMTask(LSMImage lsm) {
//        return null;
//    }
//
//    private Task createMergeTask(String tileName) {
//        return null;
//    }
//
//    private Task createGroupTask(String areaName) {
//        return null;
//    }
//
//    private Task createPostProcessingTask(String tileName) {
//        return null;
//    }
//
//    private Task createPostProcessingUpdateTask() {
//        return null;
//    }
//
//    private Task createStitchTask(String tileName) {
//        return null;
//    }
//
//    private Task createArtifactTask(String tileName) {
//        return null;
//    }
//
//    private Task createArtifactUpdateTask() {
//        return null;
//    }





    private String getArea(String tile) {
        return null;
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

package org.janelia.jacs2.asyncservice.workflow;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.mxgraph.layout.hierarchical.mxHierarchicalLayout;
import com.mxgraph.layout.mxIGraphLayout;
import com.mxgraph.model.mxCell;
import com.mxgraph.swing.mxGraphComponent;
import com.mxgraph.view.mxGraph;
import org.janelia.dagobah.DAG;
import org.janelia.dagobah.Task;
import org.janelia.dagobah.WorkflowProcessorKt;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.sample.LSMImage;
import org.janelia.model.domain.sample.Sample;
import org.janelia.model.domain.workflow.SamplePipelineConfiguration;
import org.janelia.model.domain.workflow.WorkflowTask;

import javax.swing.*;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class WorkflowViewer extends JFrame {

    private boolean showInputs = false;
    private Map<Long, Object> vertices = new HashMap<>();

    public WorkflowViewer(DAG<WorkflowTask> dag) {

        mxGraph graph = new mxGraph() {
            // Overrides method to disallow label editing
            @Override
            public boolean isCellEditable(Object cell) {
                return false;
            }

            // Overrides method to provide a cell label in the display
            @Override
            public String convertValueToString(Object cell) {
                if (cell instanceof mxCell) {
                    Object value = ((mxCell) cell).getValue();

                    if (value instanceof WorkflowTask) {
                        WorkflowTask task = (WorkflowTask) value;
                        String label = task.getName().replaceFirst(" \\(", "<br>(");
                        return "<html>" + label + "</html>";
                    }
                }

                return super.convertValueToString(cell);
            }
        };

        graph.setHtmlLabels(true);

        graph.getModel().beginUpdate();
        Object parent = graph.getDefaultParent();

        try {
            Integer edgeId = 1;

            Set<Long> taskIdsToRun = WorkflowProcessorKt.getTasksToRun(dag).stream().map(Task::getNodeId).collect(Collectors.toSet());

            for (WorkflowTask task : dag.getNodes()) {

                String fillColor;
                if (task.getHasEffects()) {
                    fillColor = "#fbdbff"; // light pink
                } else {
                    fillColor = "#c6eaff"; // light blue
                }

                String strokeColor;
                if (taskIdsToRun.contains(task.getNodeId())) {
                    strokeColor = "#ff0000"; // red border if running
                } else {
                    // Not running, greyed out
                    fillColor = "#eeeeee";
                    strokeColor = "#888888";
                }

                String style = "ROUNDED;strokeColor=" + strokeColor + ";fillColor=" + fillColor;

                Object vertex = graph.insertVertex(parent, task.getId().toString(),
                        task, 20, 20, 160, 30, style);
                vertices.put(task.getId(), vertex);

                if (showInputs) {
                    for (String inputKey : task.getInputs().keySet()) {
                        Object value = task.getInputs().get(inputKey);
                        String label = "<html>" + inputKey + "<br>" + value + "</html>";
                        Object inputVertex = graph.insertVertex(parent, task.getId().toString() + "_" + inputKey,
                                label, 20, 20, 150, 30);
                        graph.insertEdge(parent, edgeId + "", "", inputVertex, vertex);
                        edgeId++;
                    }
                }
            }

            Map<Long, Set<Long>> edges = dag.getEdges();
            for (Long sourceId : edges.keySet()) {
                for (Long targetId : edges.get(sourceId)) {
                    Object v1 = vertices.get(sourceId);
                    Object v2 = vertices.get(targetId);
                    graph.insertEdge(parent, edgeId + "", "", v1, v2);
                    edgeId++;
                }
            }

        } finally {
            graph.getModel().endUpdate();
        }

        mxIGraphLayout layout = new mxHierarchicalLayout(graph);
        layout.execute(graph.getDefaultParent());

        mxGraphComponent graphComponent = new mxGraphComponent(graph);
        add(graphComponent);
    }

    public static void main(String args[]) throws IOException {

//        String sampleId = "1927508702504419426";
//        String sampleId = "1978637843060228194";
        String sampleId = "2416765320409645154";
//        String sampleId = "2533483410182111330";

        String TEST_DATADIR = "src/test/resources/testdata/samples";
        Path testDataDir = Paths.get(TEST_DATADIR);
        ObjectMapper mapper = new ObjectMapper();
        Path jsonFile = testDataDir.resolve("sampleandlsm_" + sampleId + ".json");
        List<DomainObject> objects = mapper.readValue(jsonFile.toFile(), new TypeReference<List<DomainObject>>() {
        });

        Sample sample = getSample(objects);
        List<LSMImage> lsms = getImages(objects);
        SampleWorkflow workflow = new SampleWorkflow(new SamplePipelineConfiguration(), Sets.newHashSet());

        DAG<WorkflowTask> dag = workflow.createPipeline(sample, lsms);

        WorkflowViewer frame = new WorkflowViewer(dag);
        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }

    private static List<LSMImage> getImages(List<DomainObject> objects) {
        return objects.stream().filter((object) -> object instanceof LSMImage).map((obj) -> (LSMImage) obj).collect(Collectors.toList());
    }

    private static Sample getSample(List<DomainObject> objects) {
        return objects.stream().filter((object) -> object instanceof Sample).map((obj) -> (Sample) obj).collect(Collectors.toList()).get(0);
    }
}
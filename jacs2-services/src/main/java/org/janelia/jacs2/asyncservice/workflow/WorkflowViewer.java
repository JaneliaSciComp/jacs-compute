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
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.sample.LSMImage;
import org.janelia.model.domain.sample.Sample;
import org.janelia.model.domain.sample.SamplePipelineRun;
import org.janelia.model.domain.workflow.SamplePipelineConfiguration;
import org.janelia.model.domain.workflow.WorkflowTask;

import javax.swing.*;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class WorkflowViewer extends JFrame {

    private static final int NODE_PADDING = 10;
    private static final int NODE_WIDTH = 190;
    private static final int NODE_HEIGHT = 50;

    private boolean showInputs = true;
    private mxGraph graph;
    private Map<Long, Object> vertices = new HashMap<>();
    private Set<String> creation = new HashSet<>();
    private Integer edgeId = 1;

    public WorkflowViewer(DAG<WorkflowTask> dag) {

        this.graph = new mxGraph() {
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
//                        String label = task.getName().replaceFirst(" \\(", "<br>(");
                        String label = task.getName();
                        if (task.getHasEffects()) {
                            label += "<br><b>Has Effects</b>";
                        }
                        if (task.getForce()) {
                            label += "<br><b>Forced</b>";
                        }
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

            updateNodes(dag);
//            for (WorkflowTask task : dag.getNodes()) {
//
//                Object vertex = graph.insertVertex(parent, task.getId().toString(),
//                        task, NODE_PADDING, NODE_PADDING, NODE_WIDTH, NODE_HEIGHT, "");
//                vertices.put(task.getId(), vertex);
//
//                if (showInputs) {
//                    for (String inputKey : task.getInputs().keySet()) {
//                        Object value = task.getInputs().get(inputKey);
//                        String label = "<html>" + inputKey + "<br>" + value + "</html>";
//                        Object inputVertex = graph.insertVertex(parent, task.getId().toString() + "_" + inputKey,
//                                label, NODE_PADDING, NODE_PADDING, NODE_WIDTH, NODE_HEIGHT, getInputStyle());
//                        graph.insertEdge(parent, edgeId + "", "", inputVertex, vertex);
//                        edgeId++;
//                    }
//                }
//            }


        } finally {
            graph.getModel().endUpdate();
        }

        // Run layout
//        mxIGraphLayout layout = new mxHierarchicalLayout(graph);
//        layout.execute(graph.getDefaultParent());

        mxGraphComponent graphComponent = new mxGraphComponent(graph);
        add(graphComponent);

//        updateNodes(dag);
    }

    public void updateNodes(DAG<WorkflowTask> dag) {

        try {
            graph.getModel().beginUpdate();
            Object parent = graph.getDefaultParent();

            for (WorkflowTask task : dag.getNodes()) {
                Object vertex = vertices.get(task.getId());
                if (vertex==null) {
                    vertex = graph.insertVertex(parent, task.getId().toString(),
                            task, NODE_PADDING, NODE_PADDING, NODE_WIDTH, NODE_HEIGHT, "");
                    vertices.put(task.getId(), vertex);
                }
                graph.setCellStyle(getStyle(task), Arrays.asList(vertex).toArray());

                if (showInputs) {
                    for (String inputKey : task.getInputs().keySet()) {
                        String key = task.getId()+"_"+inputKey;
                        if (!creation.contains(key)) {
                            creation.add(key);
                            Object value = task.getInputs().get(inputKey);
                            if (value instanceof Collection) {
                                int size = ((Collection)value).size();
                                value = size+" items";
                            }
                            String label = "<html>" + inputKey + "<br>" + value + "</html>";
                            Object inputVertex = graph.insertVertex(parent, task.getId().toString() + "_" + inputKey,
                                    label, NODE_PADDING, NODE_PADDING, NODE_WIDTH, NODE_HEIGHT, getInputStyle());
                            graph.insertEdge(parent, edgeId + "", "", inputVertex, vertex);
                            edgeId++;
                        }

                    }
                }
            }

            Map<Long, Set<Long>> edges = dag.getEdges();
            for (Long sourceId : edges.keySet()) {
                for (Long targetId : edges.get(sourceId)) {
                    String key = sourceId+"_"+targetId;
                    if (!creation.contains(key)) {
                        creation.add(key);
                        Object v1 = vertices.get(sourceId);
                        Object v2 = vertices.get(targetId);
                        graph.insertEdge(parent, edgeId + "", "", v1, v2);
                        edgeId++;
                    }
                }
            }

            // Run layout
            mxIGraphLayout layout = new mxHierarchicalLayout(graph);
            layout.execute(graph.getDefaultParent());

        } finally {
            graph.getModel().endUpdate();
        }
    }

    private String getStyle(Task task) {

        // Default is gray
        String fillColor = "#eeeeee";
        String strokeColor = "#888888";

        switch (task.getStatus()) {
            case Pending:
                break;
            case Running:
                fillColor = "#c6eaff"; // light blue
                strokeColor = "#000000"; // black border
                break;
            case Complete:
                fillColor = "#e3ffc7"; // light green
                break;
            case Error:
                fillColor = "#fbdbff"; // light pink
                break;
        }

        return "ROUNDED;strokeColor=" + strokeColor + ";fillColor=" + fillColor;
    }

    private String getInputStyle() {
        return "ROUNDED;strokeColor=#888888;fillColor=#ffffff";
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
        List<SamplePipelineRun> pipelineRuns = Arrays.asList();

        SampleWorkflowGenerator workflow = new SampleWorkflowGenerator(new SamplePipelineConfiguration(), Sets.newHashSet());
        DAG<WorkflowTask> dag = workflow.createPipeline(sample, lsms, pipelineRuns);

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
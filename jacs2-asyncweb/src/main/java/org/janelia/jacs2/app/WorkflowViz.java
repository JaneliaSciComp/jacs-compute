package org.janelia.jacs2.app;

import org.janelia.dagobah.DAG;
import org.janelia.jacs2.asyncservice.workflow.WorkflowViewer;
import org.janelia.model.access.domain.WorkflowDAO;
import org.janelia.model.domain.workflow.Workflow;
import org.janelia.model.domain.workflow.WorkflowTask;

import javax.enterprise.inject.Default;
import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.inject.Inject;
import javax.swing.*;
import java.util.Timer;
import java.util.TimerTask;


@Default
public class WorkflowViz {

    @Inject
    private WorkflowDAO workflowDAO;

    public void view(Long workflowId) {

        DAG<WorkflowTask> dag = getDAG(workflowId);
        WorkflowViewer frame = new WorkflowViewer(dag);

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                DAG<WorkflowTask> dag = getDAG(workflowId);
                frame.updateNodes(dag);
            }
        }, 0, 1000);

        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }

    private DAG<WorkflowTask> getDAG(Long workflowId) {
        Workflow workflow = workflowDAO.getWorkflow(workflowId);
        return workflowDAO.getDAG(workflow);
    }

    //
    // Currently for this to work in IntelliJ, you have to copy this manually once:
    // cp -R jacs2-services/out/production/resources/META-INF jacs2-services/out/production/classes/
    //
    // There is a potential workaround here: https://intellij-support.jetbrains.com/hc/en-us/community/posts/115000603804-Running-Weld-CDI-under-IDEA-and-beans-xml-placement
    //
    public static void main(String... args) {

        String workflowId = "2580226879638208533";

//        SeContainer container = SeContainerFactory.getSeContainer();

//        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
//        SeContainer container = containerInit.initialize();

//        Weld weld = new Weld();
//        WeldContainer container = weld.initialize();

        // None of the above works, we need to add the class manually, I'm not sure why.
        SeContainerInitializer containerInit = SeContainerInitializer
                .newInstance()
                .addBeanClasses(WorkflowViz.class);
        SeContainer container = containerInit.initialize();

        WorkflowViz workflowViewer = container.select(WorkflowViz.class).get();
        workflowViewer.view(Long.parseLong(workflowId));

        container.close();
    }
}
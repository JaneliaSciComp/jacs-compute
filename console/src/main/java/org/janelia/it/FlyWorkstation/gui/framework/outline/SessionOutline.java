package org.janelia.it.FlyWorkstation.gui.framework.outline;

import org.janelia.it.FlyWorkstation.gui.framework.api.EJBFactory;
import org.janelia.it.FlyWorkstation.gui.framework.console.ConsoleFrame;
import org.janelia.it.jacs.compute.api.ComputeBeanRemote;
import org.janelia.it.jacs.model.tasks.Task;
import org.janelia.it.jacs.model.tasks.TaskParameter;
import org.janelia.it.jacs.model.tasks.annotation.AnnotationSessionTask;

import javax.swing.*;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;
import javax.swing.tree.TreeSelectionModel;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: saffordt
 * Date: 2/8/11
 * Time: 2:09 PM
 * This class is the initial outline of the data file tree
 */
public class SessionOutline extends JScrollPane implements Cloneable {
    public static final String NO_DATASOURCE = "No Tasks Available";
    private ConsoleFrame consoleFrame;
    private JPopupMenu popupMenu;
    private DynamicTree treePanel;
    private static final String ANNOTATION_SESSIONS = "Annotation Sessions";

    public SessionOutline(ConsoleFrame consoleFrame) {
        this.consoleFrame = consoleFrame;
        popupMenu = new JPopupMenu();
        popupMenu.setLightWeightPopupEnabled(true);
        treePanel = new DynamicTree(ANNOTATION_SESSIONS);
        //rebuildTreeModel();
        treePanel.getTree().addTreeSelectionListener((new TreeSelectionListener() {
            public void valueChanged(TreeSelectionEvent treeSelectionEvent) {
//                System.out.println("Selected "+treeSelectionEvent.getPath());
                TreePath tmpPath = treeSelectionEvent.getPath();
                if (tmpPath.getLastPathComponent().toString().equals(NO_DATASOURCE)) {return;}
                String tmpTask = tmpPath.getLastPathComponent().toString();
                if (null!=tmpTask && !"".equals(tmpTask)) {
                    SessionOutline.this.consoleFrame.setMostRecentFileOutlinePath(tmpTask);
                }
            }
        }));
        treePanel.getTree().addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent mouseEvent) {
                handleMouseEvents(mouseEvent);
            }
        });
        // todo Change the root to not visible
        treePanel.getTree().setRootVisible(true);
        treePanel.getTree().getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);
        rebuildDataModel();
    }

    private void rebuildTreeModel(){
        DefaultMutableTreeNode newRootNode = buildTreeModel();
        DefaultTreeModel newModel = new DefaultTreeModel(newRootNode);
        treePanel.getTree().setModel(newModel);
    }

    private void handleMouseEvents(MouseEvent e) {
        if (null==treePanel.getTree() || null==treePanel.getTree().getLastSelectedPathComponent()) return;
        String treePath = treePanel.getTree().getLastSelectedPathComponent().toString();
        if ((e.getModifiers() & MouseEvent.BUTTON3_MASK) > 0) {
            System.out.println("SessionOutline Rt. button mouse pressed clicks: " + e.getClickCount() + " " + System.currentTimeMillis());
//            if (treePath.equals(ANNOTATION_SESSIONS)) {
//                getAnnotationPopupMenu(e);
//            }
        }
        if (treePanel.getTree().getLastSelectedPathComponent() instanceof DefaultMutableTreeNode) {    //if not a DefaultMutableTreeNode, punt
            DefaultMutableTreeNode node = (DefaultMutableTreeNode) treePanel.getTree().getLastSelectedPathComponent();
            Object userObj = node.getUserObject();
//            treePanel.getTree().setSelectionPath(previousTreeSelectionPath);
        }
    }

//    private void getAnnotationPopupMenu(MouseEvent e) {
//        actionPopup = new JPopupMenu();
//        JMenuItem newAnnotationSessionButton = new JMenuItem("New Annotation Session");
//        newAnnotationSessionButton.addActionListener(new ActionListener() {
//            public void actionPerformed(ActionEvent actionEvent) {
//                SessionTask newSession = new SessionTask();
//
//                System.out.println("DEBUG: " + tmpCmd);
//                try {
//                    Runtime.getRuntime().exec(tmpCmd);
//                }
//                catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//        JMenuItem stackInfoItem = new JMenuItem("Show Image Info");
//        stackInfoItem.addActionListener(new ActionListener() {
//            public void actionPerformed(ActionEvent actionEvent) {
//                System.out.println("Calling for tree info...");
//                JOptionPane.showMessageDialog(actionPopup, "Calling for TIF Info...", "Show Image Info", JOptionPane.PLAIN_MESSAGE);
//            }
//        });
//        actionPopup.add(v3dMenuItem);
//        if (treePath.getAbsolutePath().toLowerCase().endsWith(".tif")|| treePath.getAbsolutePath().toLowerCase().endsWith(".lsm")) {
//            actionPopup.add(stackInfoItem);
//        }
//        actionPopup.show(tree, e.getX(), e.getY());
//    }

    private DefaultMutableTreeNode buildTreeModel() {
        // Prep the null node, just in case
        DefaultMutableTreeNode nullNode = new DefaultMutableTreeNode(NO_DATASOURCE);
        nullNode.setUserObject(NO_DATASOURCE);
        nullNode.setAllowsChildren(false);
        try {
            ComputeBeanRemote computeBean = EJBFactory.getRemoteComputeBean();
            if (null!=computeBean) {
                DefaultMutableTreeNode top = new DefaultMutableTreeNode();
                try {
                    List<Task> tmpTasks = computeBean.getUserTasksByType(AnnotationSessionTask.TASK_NAME, System.getenv("USER"));
                    if (null==tmpTasks || tmpTasks.size()<=0) {
                        return nullNode;
                    }
                    top.setUserObject(ANNOTATION_SESSIONS);
                    for (int i = 0; i < tmpTasks.size(); i++) {
                        DefaultMutableTreeNode tmpNode = new DefaultMutableTreeNode(tmpTasks.get(i).getObjectId());
                        top.insert(tmpNode,i);
                        // Add the properties under the items
                        int paramCount = 0;
                        for (TaskParameter tmpParam : tmpTasks.get(i).getTaskParameterSet()) {
                            tmpNode.insert(new DefaultMutableTreeNode(tmpParam.getName()+":"+tmpParam.getValue()),paramCount);
                            paramCount++;
                        }
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                return top;
            }
            return nullNode;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return nullNode;
    }

    public void rebuildDataModel() {
        // Load the tree in the background so that the app starts up first
        SwingWorker<Void, Void> loadTasks = new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
                try {
                    rebuildTreeModel();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                setViewportView(treePanel);
                return null;
            }
        };

        loadTasks.execute();
    }


    public void selectSession(String currentAnnotationSessionTaskId) {
        DefaultMutableTreeNode rootNode = (DefaultMutableTreeNode) treePanel.getTree().getModel().getRoot();
        selectSessionNode(rootNode, currentAnnotationSessionTaskId);
    }

    private boolean selectSessionNode(DefaultMutableTreeNode rootNode, String currentAnnotationSessionTaskId) {
        if (rootNode.toString().equals(currentAnnotationSessionTaskId)) {
            treePanel.getTree().getSelectionModel().setSelectionPath(new TreePath(rootNode.getPath()));
            return true;
        }
        for (int i = 0; i < rootNode.getChildCount(); i++) {
            boolean walkSuccess = selectSessionNode((DefaultMutableTreeNode)rootNode.getChildAt(i), currentAnnotationSessionTaskId);
            if (walkSuccess) {return true;}
        }
        return false;
    }

    public void clearSelection() {
        treePanel.getTree().clearSelection();
    }
}

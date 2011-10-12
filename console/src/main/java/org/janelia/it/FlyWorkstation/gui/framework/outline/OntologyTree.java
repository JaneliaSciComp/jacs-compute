package org.janelia.it.FlyWorkstation.gui.framework.outline;

import java.awt.BorderLayout;
import java.awt.event.MouseEvent;
import java.util.concurrent.Callable;

import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;

import org.janelia.it.FlyWorkstation.api.entity_model.management.ModelMgr;
import org.janelia.it.FlyWorkstation.gui.framework.tree.DynamicTree;
import org.janelia.it.FlyWorkstation.gui.util.Icons;
import org.janelia.it.FlyWorkstation.gui.util.SimpleWorker;
import org.janelia.it.jacs.model.entity.Entity;
import org.janelia.it.jacs.model.ontology.OntologyElement;
import org.janelia.it.jacs.model.ontology.OntologyRoot;


/**
 * An tree of ontology terms, which knows how to loads ontologies from the Entity data model.
 * <p/>
 * TODO: allow lazy loading similar to how EntityTree works
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class OntologyTree extends JPanel {

    protected final JPanel treesPanel;
    protected DynamicTree selectedTree;

    public OntologyTree() {
        super(new BorderLayout());

        treesPanel = new JPanel(new BorderLayout());
        add(treesPanel, BorderLayout.CENTER);
    }

    public void showLoadingIndicator() {
        treesPanel.removeAll();
        treesPanel.add(new JLabel(Icons.getLoadingIcon()));
        this.updateUI();
    }

    public OntologyRoot getCurrentOntology() {
    	if (selectedTree == null) return null;
        return (OntologyRoot) selectedTree.getRootNode().getUserObject();
    }

    public void clearTree() {
        treesPanel.removeAll();
    }

    public void initializeTree(final Long rootId, final Callable<Void> success) {

        treesPanel.removeAll();

        if (rootId == null) return;

        showLoadingIndicator();

        SimpleWorker loadingWorker = new SimpleWorker() {

            private Entity rootEntity;

            protected void doStuff() throws Exception {
                rootEntity = ModelMgr.getModelMgr().getOntologyTree(rootId);
            }

            protected void hadSuccess() {
                try {
                    initializeTree(new OntologyRoot(rootEntity));
//                    success.call();
                }
                catch (Exception e) {
                    hadError(e);
                }
            }

            protected void hadError(Throwable error) {
                error.printStackTrace();
                JOptionPane.showMessageDialog(OntologyTree.this, "Error loading ontology", "Ontology Load Error", JOptionPane.ERROR_MESSAGE);
                treesPanel.removeAll();
                OntologyTree.this.updateUI();
            }

        };

        loadingWorker.execute();
    }

    public void initializeTree(final OntologyRoot root) {

        // Create a new tree and add all the nodes to it

        createNewTree(root);
        addNodes(null, root);

        // Replace the tree in the panel

        treesPanel.removeAll();
        treesPanel.add(selectedTree);

        // Prepare for display and update the UI

        selectedTree.expandAll(true);
        OntologyTree.this.updateUI();
    }

    public DynamicTree getDynamicTree() {
        return selectedTree;
    }

    public JTree getTree() {
        return selectedTree.getTree();
    }

    /**
     * Override this method to show a popup menu when the user right clicks a node in the tree.
     *
     * @param e
     */
    protected void showPopupMenu(MouseEvent e) {
    }

    /**
     * Override this method to do something when the user left clicks a node.
     *
     * @param e
     */
    protected void nodeClicked(MouseEvent e) {
    }

    /**
     * Override this method to do something when the user presses down on a node.
     *
     * @param e
     */
    protected void nodePressed(MouseEvent e) {
    }

    /**
     * Override this method to do something when the user double clicks a node.
     *
     * @param e
     */
    protected void nodeDoubleClicked(MouseEvent e) {
    }

    protected void createNewTree(OntologyRoot root) {

        selectedTree = new DynamicTree(root, true, false) {

            protected void showPopupMenu(MouseEvent e) {
                OntologyTree.this.showPopupMenu(e);
            }

            protected void nodeClicked(MouseEvent e) {
                OntologyTree.this.nodeClicked(e);
            }

            protected void nodePressed(MouseEvent e) {
                OntologyTree.this.nodePressed(e);
            }

            protected void nodeDoubleClicked(MouseEvent e) {
                OntologyTree.this.nodeDoubleClicked(e);
            }

        };

        // Replace the cell renderer

        selectedTree.setCellRenderer(new OntologyTreeCellRenderer());
    }

    protected void addNodes(DefaultMutableTreeNode parentNode, OntologyElement element) {

        // Add the node to the tree
        DefaultMutableTreeNode newNode;
        if (parentNode != null) {
            newNode = selectedTree.addObject(parentNode, element);
        }
        else {
            // If the parent node is null, then the node is already in the tree as the root
            newNode = selectedTree.getRootNode();
        }

        // Add the node's children.
        // They are available because the root was loaded with the eager-loading getOntologyTree() method.
        for (OntologyElement child : element.getChildren()) {
            addNodes(newNode, child);
        }
    }

    public void navigateToOntologyElement(OntologyElement element) {
        selectedTree.navigateToNodeWithObject(element);
    }

    /**
     * @return true if the user is allowed to edit the current ontology, false otherwise.
     */
    public boolean isEditable() {
        return !getCurrentOntology().isPublic();
    }

}

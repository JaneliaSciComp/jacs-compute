package org.janelia.it.FlyWorkstation.gui.framework.outline;

import org.janelia.it.FlyWorkstation.gui.application.ConsoleApp;
import org.janelia.it.FlyWorkstation.gui.framework.keybind.KeyboardShortcut;
import org.janelia.it.FlyWorkstation.gui.framework.keybind.KeymapUtil;
import org.janelia.it.jacs.model.entity.Entity;

import javax.swing.*;
import javax.swing.tree.*;
import java.awt.*;
import java.util.Enumeration;

/**
 * Created by IntelliJ IDEA.
 * User: saffordt
 * Date: 6/1/11
 * Time: 4:55 PM
 */
class DynamicTree extends JPanel {

    protected EntityMutableTreeNode rootNode;
    protected DefaultTreeModel treeModel;
    protected JTree tree;
    private Toolkit toolkit = Toolkit.getDefaultToolkit();

   
    public DynamicTree(ActionableEntity rootEntity) {
        super(new GridLayout(1, 0));

        rootNode = new EntityMutableTreeNode(rootEntity);
        treeModel = new DefaultTreeModel(rootNode);

        tree = new JTree(treeModel);
        tree.setRowHeight(25);
        tree.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);
        tree.setShowsRootHandles(true);
        tree.setCellRenderer(new EntityCellRenderer());

        JScrollPane scrollPane = new JScrollPane(tree);
        scrollPane.setPreferredSize(new Dimension(300,800));
        add(scrollPane);

    }

    /**
     * Retrieve the underlying tree.
     */
    public JTree getTree() {
        return tree;
    }

    /**
     * Remove the currently selected node.
     */
    public void removeCurrentNode() {
        TreePath currentSelection = tree.getSelectionPath();
        if (currentSelection != null) {
            EntityMutableTreeNode currentNode = (EntityMutableTreeNode) (currentSelection.getLastPathComponent());
            MutableTreeNode parent = (MutableTreeNode) (currentNode.getParent());
            if (parent != null) {
                treeModel.removeNodeFromParent(currentNode);
                return;
            }
        }

        // Either there was no selection, or the root was selected.
        toolkit.beep();
    }

    /**
      * Get the currently selected node name.
      */
     public EntityMutableTreeNode getCurrentNode() {
         TreePath currentSelection = tree.getSelectionPath();
         if (currentSelection != null) {
             return (EntityMutableTreeNode) (currentSelection.getLastPathComponent());
         }
         return null;
     }

    /**
      * Get the currently selected node name.
      */
     public String getCurrentNodeName() {
         EntityMutableTreeNode treeNode = getCurrentNode();
         if (treeNode != null) return treeNode.getEntityName();
         return null;
     }

    /**
      * Get the currently selected node id.
      */
     public String getCurrentNodeId() {
         EntityMutableTreeNode treeNode = getCurrentNode();
         if (treeNode != null) return treeNode.getEntityId().toString();
         return null;
     }

     /**
     * Add child to the currently selected node.
     */
    public EntityMutableTreeNode addObject(Object child) {
        EntityMutableTreeNode parentNode = null;
        TreePath parentPath = tree.getSelectionPath();

        if (parentPath == null) {
            parentNode = rootNode;
        }
        else {
            parentNode = (EntityMutableTreeNode) (parentPath.getLastPathComponent());
        }

        return addObject(parentNode, child, true);
    }

    public EntityMutableTreeNode addObject(EntityMutableTreeNode parent, Object child) {
        return addObject(parent, child, false);
    }


    public EntityMutableTreeNode addObject(EntityMutableTreeNode parent, Object child, boolean shouldBeVisible) {
        EntityMutableTreeNode childNode = new EntityMutableTreeNode(child);

        if (parent == null) {
            parent = rootNode;
        }

        // It is key to invoke this on the TreeModel, and NOT EntityMutableTreeNode
        treeModel.insertNodeInto(childNode, parent, parent.getChildCount());

        // Make sure the user can see the lovely new node.
        if (shouldBeVisible) {
            tree.scrollPathToVisible(new TreePath(childNode.getPath()));
        }
        return childNode;
    }

    public void removeRootChildren() {
        try {
            TreePath currentSelection = tree.getSelectionPath();
            if (currentSelection != null) {
                EntityMutableTreeNode rootNode = (EntityMutableTreeNode) (currentSelection.getPathComponent(0));
                Enumeration enumeration = rootNode.children();
                while(enumeration.hasMoreElements()) {
                    EntityMutableTreeNode currentNode = (EntityMutableTreeNode)enumeration.nextElement();
                    MutableTreeNode parent = (MutableTreeNode) (currentNode.getParent());
                    if (parent != null) {
                        treeModel.removeNodeFromParent(currentNode);
                    }
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void expandAll() {
        // expand to the last leaf from the root
        int row = 0;
        while (row < tree.getRowCount()) {
            tree.expandRow(row);
            row++;
        }
    }

    public EntityMutableTreeNode refreshDescendants(EntityMutableTreeNode currentNode) {
        treeModel.nodeChanged(currentNode);
        Enumeration enumeration = currentNode.children();
        while(enumeration.hasMoreElements()) {
            refreshDescendants((EntityMutableTreeNode)enumeration.nextElement());
        }

        return null;
    }

    public void navigateToNextRow() {

        int[] selection = tree.getSelectionRows();
        if (selection != null && selection.length > 0) {
            int nextRow = selection[0]+1;
            if (nextRow >= tree.getRowCount()) {
                tree.setSelectionRow(0);
            }
            else {
                tree.setSelectionRow(nextRow);
            }
        }
    }

    public void navigateToEntityNode(Entity entity) {

        EntityMutableTreeNode node = getNodeForEntity(entity, (EntityMutableTreeNode)treeModel.getRoot());
        if (node == null) return;
        
        TreePath treePath = new TreePath(node.getPath());
        tree.expandPath(treePath);
        tree.setSelectionPath(treePath);
    }

    private EntityMutableTreeNode getNodeForEntity(Entity entity, EntityMutableTreeNode currentNode) {

        if (currentNode.getEntityId().equals(entity.getId())) {
            return currentNode;
        }

        Enumeration enumeration = currentNode.children();
        while(enumeration.hasMoreElements()) {
            EntityMutableTreeNode childNode = (EntityMutableTreeNode)enumeration.nextElement();
            EntityMutableTreeNode foundNode = getNodeForEntity(entity, childNode);
            if (foundNode != null) return foundNode;
        }

        return null;
    }


    /**
     * Special tree cell renderer which displays a label (icon and text) as well as a key binding next to it,
     * if one exists.
     */
    private class EntityCellRenderer extends DefaultTreeCellRenderer implements TreeCellRenderer {
        private JLabel titleLabel;
        private JLabel keybindLabel;
        private JPanel cellPanel;
        private Color foregroundSelectionColor;
        private Color foregroundNonSelectionColor;
        private Color backgroundSelectionColor;
        private Color backgroundNonSelectionColor;
        private DefaultTreeCellRenderer defaultRenderer = new DefaultTreeCellRenderer();

        public EntityCellRenderer() {
            cellPanel = new JPanel(new FlowLayout(FlowLayout.LEFT));
            titleLabel = new JLabel(" ");
            titleLabel.setOpaque(true);
            titleLabel.setBorder(BorderFactory.createEmptyBorder(2, 2, 2, 2));
            titleLabel.setForeground(Color.black);
            cellPanel.add(titleLabel);
            keybindLabel = new JLabel(" ");
            keybindLabel.setForeground(Color.gray);
            cellPanel.add(keybindLabel);
            foregroundSelectionColor = defaultRenderer.getTextSelectionColor();
            foregroundNonSelectionColor = defaultRenderer.getTextNonSelectionColor();
            backgroundSelectionColor = defaultRenderer
                    .getBackgroundSelectionColor();
            backgroundNonSelectionColor = defaultRenderer
                    .getBackgroundNonSelectionColor();

            cellPanel.setBackground(backgroundNonSelectionColor);
        }

        public Component getTreeCellRendererComponent(JTree tree, Object value, boolean selected,
                                                      boolean expanded, boolean leaf, int row, boolean hasFocus) {
            Component returnValue = null;
            if ((value != null) && (value instanceof DefaultMutableTreeNode)) {
                Object userObject = ((DefaultMutableTreeNode) value)
                        .getUserObject();
                if (userObject instanceof ActionableEntity) {
                    ActionableEntity node = (ActionableEntity)userObject;

                    titleLabel.setText(node.getEntity().getName());

                    KeyboardShortcut bind = ConsoleApp.getKeyBindings().getBinding(node.getAction());
                    if (bind != null) {
                        keybindLabel.setText("("+KeymapUtil.getShortcutText(bind)+")");
                    }
                    else {
                        keybindLabel.setText("");
                    }

                    if (selected) {
                        titleLabel.setForeground(foregroundSelectionColor);
                        titleLabel.setBackground(backgroundSelectionColor);
                    } else {
                        titleLabel.setForeground(foregroundNonSelectionColor);
                        titleLabel.setBackground(backgroundNonSelectionColor);
                    }

                    cellPanel.setEnabled(tree.isEnabled());
                    if (leaf)
                      titleLabel.setIcon(getLeafIcon());
                    else if (expanded)
                      titleLabel.setIcon(getOpenIcon());
                    else
                      titleLabel.setIcon(getClosedIcon());

                    returnValue = cellPanel;
                }
            }
            if (returnValue == null) {
                returnValue = defaultRenderer.getTreeCellRendererComponent(tree,
                        value, selected, expanded, leaf, row, hasFocus);
            }
            return returnValue;
        }
    }

}
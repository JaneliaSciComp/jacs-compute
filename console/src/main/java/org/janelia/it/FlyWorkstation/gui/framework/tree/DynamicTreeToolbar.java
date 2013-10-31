package org.janelia.it.FlyWorkstation.gui.framework.tree;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.JToolBar;
import javax.swing.tree.DefaultMutableTreeNode;

import org.janelia.it.FlyWorkstation.gui.framework.session_mgr.SessionMgr;
import org.janelia.it.FlyWorkstation.gui.util.Icons;

/**
 * A toolbar which sits on top of a DynamicTree and provides generic tree-related functions such as
 * expanding/collapsing all nodes in the tree, and refreshing the tree.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class DynamicTreeToolbar extends JPanel implements ActionListener {

    private static final String EXPAND_ALL = "expand_all";
    private static final String COLLAPSE_ALL = "collapse_all";
    private static final String REFRESH = "refresh";

    private final DynamicTree tree;
    private JTextField textField;
    private JButton expandAllButton;
    private JButton collapseAllButton;
    private JButton refreshButton;
    private JLabel spinner;
    private JToolBar toolBar;

    public DynamicTreeToolbar(final DynamicTree tree) {
        super(new BorderLayout());

        this.tree = tree;
        this.toolBar = new JToolBar();
        toolBar.setFloatable(false);
        toolBar.setRollover(true);
        
        expandAllButton = new JButton(Icons.getExpandAllIcon());
        expandAllButton.setActionCommand(EXPAND_ALL);
        expandAllButton.setToolTipText("Expand all the nodes in the tree.");
        expandAllButton.addActionListener(this);
        expandAllButton.setFocusable(false);
        toolBar.add(expandAllButton);

        collapseAllButton = new JButton(Icons.getCollapseAllIcon());
        collapseAllButton.setActionCommand(COLLAPSE_ALL);
        collapseAllButton.setToolTipText("Collapse all the nodes in the tree.");
        collapseAllButton.addActionListener(this);
        collapseAllButton.setFocusable(false);
        toolBar.add(collapseAllButton);

        refreshButton = new JButton(Icons.getRefreshIcon());
        refreshButton.setActionCommand(REFRESH);
        refreshButton.setToolTipText("Refresh the data in the tree.");
        refreshButton.addActionListener(this);
        refreshButton.setFocusable(false);
        toolBar.add(refreshButton);
        
        add(toolBar, BorderLayout.PAGE_START);
    }

    public void actionPerformed(ActionEvent e) {
        String cmd = e.getActionCommand();
        if (EXPAND_ALL.equals(cmd)) {
        	DefaultMutableTreeNode node = tree.getCurrentNode();
        	if (node==null) node = tree.getRootNode();
        	if (tree.isLazyLoading() && node==tree.getRootNode()) {
                int deleteConfirmation = JOptionPane.showConfirmDialog(SessionMgr.getSessionMgr().getActiveBrowser(), 
                		"Expanding the entire tree may take a long time. Are you sure you want to do this?", 
                		"Expand All", JOptionPane.YES_NO_OPTION);
                if (deleteConfirmation != 0) {
                    return;
                }
        	}
            expandAllButton.setEnabled(false);
            collapseAllButton.setEnabled(false);
            tree.expandAll(node, true);
            expandAllButton.setEnabled(true);
            collapseAllButton.setEnabled(true);
        }
        else if (COLLAPSE_ALL.equals(cmd)) {
        	DefaultMutableTreeNode node = tree.getCurrentNode();
        	if (node==null) node = tree.getRootNode();
        	if (tree.getCurrentNode()==null) return;
            collapseAllButton.setEnabled(false);
            expandAllButton.setEnabled(false);
            tree.expandAll(node, false);
            collapseAllButton.setEnabled(true);
            expandAllButton.setEnabled(true);
        }
        else if (REFRESH.equals(cmd)) {
        	tree.totalRefresh();
        }
    }

	public JTextField getTextField() {
		return textField;
	}
	
	public void setSpinning(boolean spin) {
        spinner.setIcon(spin ? Icons.getLoadingIcon() : null);
	}

    public JToolBar getJToolBar() {
        return toolBar;
    }
}

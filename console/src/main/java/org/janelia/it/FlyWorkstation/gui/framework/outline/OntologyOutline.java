package org.janelia.it.FlyWorkstation.gui.framework.outline;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.InputEvent;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JToggleButton;
import javax.swing.JTree;
import javax.swing.SwingWorker;
import javax.swing.tree.DefaultTreeModel;

import org.janelia.it.FlyWorkstation.gui.application.ConsoleApp;
import org.janelia.it.FlyWorkstation.gui.framework.actions.Action;
import org.janelia.it.FlyWorkstation.gui.framework.actions.AnnotateAction;
import org.janelia.it.FlyWorkstation.gui.framework.actions.NavigateToNodeAction;
import org.janelia.it.FlyWorkstation.gui.framework.actions.OntologyTermAction;
import org.janelia.it.FlyWorkstation.gui.framework.api.EJBFactory;
import org.janelia.it.FlyWorkstation.gui.framework.keybind.KeyBindFrame;
import org.janelia.it.FlyWorkstation.gui.framework.keybind.KeybindChangeEvent;
import org.janelia.it.FlyWorkstation.gui.framework.keybind.KeybindChangeListener;
import org.janelia.it.FlyWorkstation.gui.framework.keybind.KeyboardShortcut;
import org.janelia.it.FlyWorkstation.gui.framework.keybind.KeymapUtil;
import org.janelia.it.jacs.compute.access.DaoException;
import org.janelia.it.jacs.model.entity.Entity;
import org.janelia.it.jacs.model.entity.EntityConstants;
import org.janelia.it.jacs.model.entity.EntityData;
import org.janelia.it.jacs.model.ontology.Category;
import org.janelia.it.jacs.model.ontology.Enum;
import org.janelia.it.jacs.model.ontology.EnumItem;
import org.janelia.it.jacs.model.ontology.Interval;
import org.janelia.it.jacs.model.ontology.OntologyTermType;
import org.janelia.it.jacs.model.ontology.Tag;
import org.janelia.it.jacs.model.ontology.Text;
import org.janelia.it.jacs.model.user_data.User;
import org.janelia.it.jacs.model.user_data.prefs.UserPreference;


/**
 * Created by IntelliJ IDEA.
 * User: saffordt
 * Date: 6/1/11
 * Time: 4:54 PM
 */
public class OntologyOutline extends JPanel implements ActionListener, KeybindChangeListener {
	
	private static final String KEYBIND_PREF_CATEGORY = "Keybind";
	
    private static final String ADD_COMMAND       = "add";
    private static final String REMOVE_COMMAND    = "remove";
    private static final String ROOT_COMMAND      = "root";
    private static final String SWITCH_COMMAND    = "switch";
    private static final String BIND_EDIT_COMMAND = "change_bind";
    private static final String BIND_MODE_COMMAND = "bind_mode";
    private static final String DELIMITER = "#";

    private Map<String,KeyboardShortcut> entityId2Shortcut = new HashMap<String,KeyboardShortcut>();
    
    private JPanel treesPanel;
    private DynamicTree selectedTree;
    private KeyBindFrame keyBindDialog;
    private JToggleButton keyBindButton;

    private final JPopupMenu popupMenu;
    private final JMenu addMenuPopup;
    private final JMenu addItemPopup;

    private List<Class<? extends OntologyTermType>> nodeTypes = new ArrayList<Class<? extends OntologyTermType>>();
    private MouseListener mouseListener;


    public OntologyOutline() {
        super(new BorderLayout());

        nodeTypes.add(Category.class);
        nodeTypes.add(Tag.class);
        nodeTypes.add(Enum.class);
        nodeTypes.add(Interval.class);
        nodeTypes.add(Text.class);

        // Create the components

        JButton newButton = new JButton("New Ontology");
        newButton.setActionCommand(ROOT_COMMAND);
        newButton.addActionListener(this);

        JButton switchButton = new JButton("Switch Ontology");
        switchButton.setActionCommand(SWITCH_COMMAND);
        switchButton.addActionListener(this);

        keyBindButton = new JToggleButton("Set Shortcuts");
        keyBindButton.setActionCommand(BIND_MODE_COMMAND);
        keyBindButton.addActionListener(this);

        // Create context menus

        popupMenu = new JPopupMenu();
        popupMenu.setLightWeightPopupEnabled(true);

        JMenuItem mi = new JMenuItem("Assign shortcut...");
        mi.addActionListener(this);
        mi.setActionCommand(BIND_EDIT_COMMAND);
        popupMenu.add(mi);

        addMenuPopup = new JMenu("Add...");
        for(Class<? extends OntologyTermType> nodeType : nodeTypes) {
			try {
				JMenuItem smi = new JMenuItem(nodeType.newInstance().getName());
				smi.addActionListener(this);
				smi.setActionCommand(ADD_COMMAND + DELIMITER + nodeType.getSimpleName());
				addMenuPopup.add(smi);
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
        
        // Prepare an alternative submenu for enumeration nodes
        addItemPopup = new JMenu("Add...");
        JMenuItem smi = new JMenuItem("Item");
        smi.addActionListener(this);
        smi.setActionCommand(ADD_COMMAND+DELIMITER+EnumItem.class.getSimpleName());
        addItemPopup.add(smi);

        popupMenu.add(addMenuPopup);

        mi = new JMenuItem("Remove this node");
        mi.addActionListener(this);
        mi.setActionCommand(REMOVE_COMMAND);
        popupMenu.add(mi);

        mouseListener = new MouseAdapter() {
            public void mouseReleased(MouseEvent e) {
                JTree tree = selectedTree.getTree();
                int row = tree.getRowForLocation(e.getX(), e.getY());
                if (row >= 0) {
                    tree.setSelectionRow(row);
                    if (e.isPopupTrigger()) {
                        showPopupMenu(e);
                    }
                    // This masking is to make sure that the right button is being double clicked, not left and then right or right and then left
                    else if (e.getClickCount()==2 
                    		&& e.getButton()==MouseEvent.BUTTON1 
                    		&& (e.getModifiersEx() | InputEvent.BUTTON3_MASK) == InputEvent.BUTTON3_MASK) {
                        OntologyTerm currTerm = selectedTree.getCurrentNode().getOntologyTerm();
                        if (!(currTerm.getAction() instanceof NavigateToNodeAction))
                            currTerm.getAction().doAction();
                    }
                }
            }
            public void mousePressed(MouseEvent e) {
                JTree tree = selectedTree.getTree();
                // We have to also listen for mousePressed because OSX generates the popup trigger here
                // instead of mouseReleased like any sane OS.
                int row = tree.getRowForLocation(e.getX(), e.getY());
                if (row >= 0) {
                    tree.setSelectionRow(row);
                    if (e.isPopupTrigger()) {
                        showPopupMenu(e);
                    }
                }
            }
        };

        // Lay everything out

        add(new JLabel("Ontology Editor"), BorderLayout.NORTH);
        
        this.treesPanel = new JPanel(new BorderLayout());
        add(treesPanel, BorderLayout.CENTER);

        GridBagConstraints c = new GridBagConstraints();
        JPanel panel = new JPanel(new GridBagLayout());

        c.gridx = 0;
        c.gridy = 1;
        c.fill = GridBagConstraints.HORIZONTAL;
        panel.add(switchButton, c);

        c.gridx = 1;
        c.gridy = 1;
        c.fill = GridBagConstraints.HORIZONTAL;
        panel.add(newButton, c);

        c.gridx = 0;
        c.gridy = 2;
        c.gridwidth = 2;
        c.fill = GridBagConstraints.HORIZONTAL;
        panel.add(keyBindButton, c);
        
        add(panel, BorderLayout.SOUTH);
        
        // Add ourselves as a keybind change listener
        
        ConsoleApp.getKeyBindings().add(this);

        // Prepare the key binding dialog box

        this.keyBindDialog = new KeyBindFrame();
        keyBindDialog.pack();

        keyBindDialog.addComponentListener(new ComponentAdapter() {
            @Override
            public void componentHidden(ComponentEvent e) {
                // refresh the tree in case the key bindings were updated
                DefaultTreeModel treeModel = (DefaultTreeModel)selectedTree.getTree().getModel();
                treeModel.nodeChanged(selectedTree.getCurrentNode());
            }
        });

        // Populate the tree view with the user's first tree

        // Load the tree in the background so that the app starts up first
        SwingWorker<Void, Void> loadTasks = new SwingWorker<Void, Void>() {
            @Override
            protected Void doInBackground() throws Exception {
				try {
					List<Entity> ontologyRootList = EJBFactory
							.getRemoteAnnotationBean().getUserEntitiesByType(
									System.getenv("USER"),
									EntityConstants.TYPE_ONTOLOGY_ROOT_ID);

					if (null != ontologyRootList
							&& ontologyRootList.size() >= 1) {
						initializeTree(ontologyRootList.get(0));
					}
				}
            	catch (Exception e) {
            		e.printStackTrace();
            	}
                return null;
            }
        };

        loadTasks.execute();
    }

    private DynamicTree initializeTree(Entity ontologyRoot) {

    	// Load preferences for this ontology first, so that keys can be bound during tree loading
    	
    	loadKeyBindPrefs(ontologyRoot);
    	
        // Create a new tree and add all the nodes to it

        OntologyTerm rootAE = new OntologyTerm(ontologyRoot,null);
        rootAE.setAction(new NavigateToNodeAction(rootAE));

        selectedTree = new DynamicTree(rootAE);
        addNodes(selectedTree, null, rootAE);
        selectedTree.expandAll();

        // Replace the default key listener on the tree
        
        final JTree tree = selectedTree.getTree();
        KeyListener defaultKeyListener = tree.getKeyListeners()[0];
        tree.removeKeyListener(defaultKeyListener);
        tree.addKeyListener(keyListener);

        // Set the mouse listener which keeps track of doubleclicks on nodes, and rightclicks to show the context menu

        tree.addMouseListener(mouseListener);

        // Replace the tree in the panel

        treesPanel.removeAll();
        treesPanel.add(selectedTree);

        this.updateUI();
        return selectedTree;
    }

    public void navigateToEntityNode(Entity entity) {
        selectedTree.navigateToEntityNode(entity);
    }

    /**
     * Load the key binds for a given ontology.
     * @param ontologyRoot
     */
    private void loadKeyBindPrefs(Entity ontologyRoot) {
    	
    	entityId2Shortcut.clear();

    	try {
        	User user = EJBFactory.getRemoteComputeBean().getUserByName(System.getenv("USER"));
        	Map<String,UserPreference> prefs = user.getCategoryPreferences(KEYBIND_PREF_CATEGORY+":"+ontologyRoot.getId());
        	
        	for(UserPreference pref : prefs.values()) {
        		entityId2Shortcut.put(pref.getValue(), KeyboardShortcut.fromString(pref.getName()));
        	}
    	}
    	catch (Exception e) {
    		System.out.println("Could not load user's key binding preferences");
    		e.printStackTrace();
    	}
    }
    
    /**
     * Save the key binds for the current ontology.
     */
    private void saveKeyBinds() {

    	try {
        	User user = EJBFactory.getRemoteComputeBean().getUserByName(System.getenv("USER"));
        	
            for(Map.Entry<KeyboardShortcut, Action> entry : ConsoleApp.getKeyBindings().getBindings().entrySet()) {
                if (entry.getValue() instanceof OntologyTermAction) {
                	OntologyTermAction ota = (OntologyTermAction)entry.getValue();
                	String shortcut = entry.getKey().toString();
                	Long entityId = ota.getOntologyTerm().getEntity().getId();
                	Long rootNodeId = selectedTree.getRootNode().getOntologyTerm().getEntity().getId();
                	user.setPreference(new UserPreference(shortcut, KEYBIND_PREF_CATEGORY+":"+rootNodeId, entityId.toString()));    	
                }
            }
            
            EJBFactory.getRemoteComputeBean().genericSave(user);
    	}
    	catch (Exception e) {
    		System.out.println("Could not save user's key binding preferences");
    		e.printStackTrace();
    	}
    	
    }
    
    private void addNodes(DynamicTree tree, EntityMutableTreeNode parentNode, OntologyTerm node) {
    	
    	// Check if there is a keyboard shortcut preference for this entity
    	KeyboardShortcut shortcut = entityId2Shortcut.get(node.getEntity().getId().toString());
    	if (shortcut != null) {
        	ConsoleApp.getKeyBindings().setBinding(shortcut, node.getAction());
    	}
    	
        EntityMutableTreeNode newNode;
        if (parentNode != null) {
            newNode = tree.addObject(parentNode, node);
        }
        else {
            // If the parent node is null, then the node is already in the tree as the root
            newNode = tree.rootNode;
        }
        Entity entity = node.getEntity();
        if (entity.getEntityData() != null) {
            for (EntityData tmpData : entity.getOrderedEntityData()) {
                Entity childEntity = tmpData.getChildEntity();
                if (childEntity != null) {
                    OntologyTerm ontologyTerm = new OntologyTerm(childEntity,node);
                    OntologyTermType type = ontologyTerm.getType();
                    if (type instanceof Category || type instanceof Enum) {
                        // These node types don't allow direct annotation actions, just navigate to them
                        ontologyTerm.setAction(new NavigateToNodeAction(ontologyTerm));
                    }
                    else {
                        // The other node types are annotatable
                        ontologyTerm.setAction(new AnnotateAction(ontologyTerm));
                    }

                    addNodes(tree, newNode, ontologyTerm);
                }
            }
        }
    }

    public void actionPerformed(ActionEvent e) {
        String command = e.getActionCommand();

        if (REMOVE_COMMAND.equals(command)) {
            // Remove button clicked
            int deleteConfirmation = JOptionPane.showConfirmDialog(
                    this,
                    "Are you sure you want to delete this term?",
                    "Delete Term",
                    JOptionPane.YES_NO_OPTION);
            if (deleteConfirmation!=0) {
                return;
            }
            EJBFactory.getRemoteAnnotationBean().removeOntologyTerm(System.getenv("USER"), selectedTree.getCurrentNodeId());
            updateSelectedTreeEntity();
        }
        else if (ROOT_COMMAND.equals(command)) {
            // New Root button clicked.
            String rootName = (String)JOptionPane.showInputDialog(
                    this,
                    "Ontology Root Name:\n",
                    "New Ontology",
                    JOptionPane.PLAIN_MESSAGE,
                    null,
                    null,
                    null);

            if ((rootName == null) || (rootName.length() <= 0)) {
                JOptionPane.showMessageDialog(this, "Require a valid name", "Ontology Error", JOptionPane.WARNING_MESSAGE);
                return;
            }

            Entity newOntologyRoot = EJBFactory.getRemoteAnnotationBean().createOntologyRoot(System.getenv("USER"), rootName);
            initializeTree(newOntologyRoot);
        }
        else if (SWITCH_COMMAND.equals(command)) {
            List<Entity> ontologyRootList = EJBFactory.getRemoteAnnotationBean().getUserEntitiesByType(System.getenv("USER"),
                    EntityConstants.TYPE_ONTOLOGY_ROOT_ID);
            ArrayList<String> ontologyNames = new ArrayList<String>();
            for (Entity entity : ontologyRootList) {
                ontologyNames.add(entity.getName());
            }
            String choice = (String)JOptionPane.showInputDialog(
                    this,
                    "Choose an ontology:\n",
                    "Ontology Chooser",
                    JOptionPane.PLAIN_MESSAGE,
                    null,
                    ontologyNames.toArray(),
                    ontologyNames.get(0));

            if ((choice != null) && (choice.length() > 0)) {
                for (Entity ontologyEntity : ontologyRootList) {
                    if (ontologyEntity.getName().equals(choice)) {
                        initializeTree(ontologyEntity);
                        break;
                    }
                }
            }
        }
        else if (BIND_EDIT_COMMAND.equals(command)) {
            EntityMutableTreeNode treeNode = selectedTree.getCurrentNode();
            if (treeNode != null) {
                OntologyTerm ae = treeNode.getOntologyTerm();
                if (ae != null)
                    keyBindDialog.showForAction(ae.getAction());
            }
        }
        else if (command.startsWith(ADD_COMMAND)) {

            if (selectedTree == null) {
                JOptionPane.showMessageDialog(this, "No ontology selected.");
            }

            EntityMutableTreeNode treeNode = selectedTree.getCurrentNode();
            OntologyTerm actionEntity = treeNode.getOntologyTerm();
            OntologyTermType parentType = actionEntity.getType();

            String className = command.split(DELIMITER)[1];
            OntologyTermType childType = OntologyTermType.createTypeByName(className);
            
            // Add button clicked
            String termName = (String)JOptionPane.showInputDialog(
                    this,
                    "Ontology Term:\n",
                    "Adding to "+selectedTree.getCurrentNodeName(),
                    JOptionPane.PLAIN_MESSAGE,
                    null,
                    null,
                    null);

            if ((termName == null) || (termName.length() <= 0)) {
                return;
            }

            if (childType instanceof Interval) {

                String lowerBoundStr = (String) JOptionPane.showInputDialog(
                        this,
                        "Lower bound:\n",
                        "Adding an interval",
                        JOptionPane.PLAIN_MESSAGE,
                        null,
                        null,
                        null);

                String upperBoundStr = (String) JOptionPane.showInputDialog(
                        this,
                        "Upper bound:\n",
                        "Adding an interval",
                        JOptionPane.PLAIN_MESSAGE,
                        null,
                        null,
                        null);

                try {
                    ((Interval)childType).init(lowerBoundStr, upperBoundStr);
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                    JOptionPane.showMessageDialog(this, "Invalid bounds");
                }
            }

            EJBFactory.getRemoteAnnotationBean().createOntologyTerm(System.getenv("USER"), selectedTree.getCurrentNodeId(),
                    termName, childType, null);
            
            
            if (parentType instanceof Tag) {
            	// Adding a child to a Tag, so it must be coerced into a Category
            	
            	EntityData ed = actionEntity.getEntity().getEntityDataByAttributeName(EntityConstants.ATTR_NAME_ONTOLOGY_TERM_TYPE);
            	ed.setValue(Category.class.getSimpleName());
            	
            	try {
					EJBFactory.getRemoteComputeBean().genericSave(ed);
				} catch (DaoException ex) {
					ex.printStackTrace();
				} catch (RemoteException ex) {
					ex.printStackTrace();
				}
            }
            
            updateSelectedTreeEntity();

        }
    }

    private void showPopupMenu(MouseEvent e) {

        OntologyTerm curr = selectedTree.getCurrentNode().getOntologyTerm();
        OntologyTermType type = curr.getType();

        popupMenu.remove(addMenuPopup);
        popupMenu.remove(addItemPopup);

        if (type instanceof Enum) {
        	popupMenu.add(addItemPopup);
        }
        else if (type.allowsChildren() || type instanceof Tag) {
        	popupMenu.add(addMenuPopup); 
        }

        popupMenu.show((JComponent)e.getSource(), e.getX(), e.getY());
    }
    
    // todo This is toooooooo brute-force
    private void updateSelectedTreeEntity(){
        Entity entity= EJBFactory.getRemoteAnnotationBean().getUserEntityById(System.getenv("USER"), selectedTree.rootNode.getEntityId());
        if (null!=selectedTree || entity.getName().equals(selectedTree.rootNode.getEntityName())){
            initializeTree(entity);
        }
        this.updateUI();
    }

    @Override
	public void keybindChange(KeybindChangeEvent evt) {
        // Save all the key bindings
        // TODO: in the future, save just the one that changed
        saveKeyBinds();
	}
    
	// Listen for key strokes and execute the appropriate key bindings
    private KeyListener keyListener = new KeyAdapter() {

        @Override
        public void keyPressed(KeyEvent e) {
            if (e.getID() == KeyEvent.KEY_PRESSED) {
                if (KeymapUtil.isModifier(e)) return;
                KeyboardShortcut shortcut = KeyboardShortcut.createShortcut(e);

                if (keyBindButton.isSelected()) {

                    // Set the key bind

                    OntologyTerm actionEntity = selectedTree.getCurrentNode().getOntologyTerm();
                    ConsoleApp.getKeyBindings().setBinding(shortcut, actionEntity.getAction());
                    
                    // Refresh the entire tree (another key bind may have been overridden)
                    // TODO: this is very slow on large trees...

                    JTree tree = selectedTree.getTree();
                    DefaultTreeModel treeModel = (DefaultTreeModel)tree.getModel();
                    selectedTree.refreshDescendants((EntityMutableTreeNode)treeModel.getRoot());

                    // Move to the next row

                    selectedTree.navigateToNextRow();

                }
                else {
                    ConsoleApp.getKeyBindings().executeBinding(shortcut);
                }
            }
        }
    };

}

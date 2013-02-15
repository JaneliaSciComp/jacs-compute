package org.janelia.it.FlyWorkstation.gui.viewer3d.slice_viewer;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JRadioButtonMenuItem;
import javax.swing.JSlider;
import javax.swing.JSplitPane;
import javax.swing.JToolBar;
import javax.swing.KeyStroke;
import javax.swing.SwingConstants;
import javax.swing.UIManager;
import javax.swing.border.TitledBorder;

import org.janelia.it.FlyWorkstation.gui.util.Icons;

public class QuadView extends JFrame {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected SliceViewer sliceViewer = new SliceViewer();
	protected Action panModeAction = new PanModeAction(sliceViewer);
	protected Action zoomInAction = new ZoomInAction(sliceViewer.getCamera());
	protected Action zoomMouseModeAction = new ZoomMouseModeAction(sliceViewer);
	protected Action zoomOutAction = new ZoomOutAction(sliceViewer.getCamera());
	protected Action zoomScrollModeAction = new ZoomScrollModeAction(sliceViewer);	
	
	static {
		// Use top menu bar on Mac
		if (System.getProperty("os.name").contains("Mac")) {
			  System.setProperty("apple.laf.useScreenMenuBar", "true");
			  System.setProperty("com.apple.mrj.application.apple.menu.about.name", "QuadView");
		}
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (Exception e) {
			System.out.println("Warning: Failed to set native look and feel.");
		}
	}
	
	public static void main(String[] args) {
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
            		new QuadView();
            }
        });
	}

	public QuadView() {
		setTitle("QuadView");
        setupUi();
	}
	
	protected void setupUi() {
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		createMenus();
		// Top level container - status bar vs rest
		Container container = getContentPane();
		container.setLayout(new BorderLayout(0, 0));
		container.add(createStatusBar(container), BorderLayout.SOUTH);
		// Next level - tool bar vs rest - tool bar requires BorderLayout
		Container parent = container;
		container = new JPanel();
		container.setLayout(new BorderLayout(0,0));
		container.add(createToolBar(), BorderLayout.NORTH);
		parent.add(container, BorderLayout.CENTER);
		// Next level - splitter dividing viewer from controls
		parent = container;
		JPanel viewerPanel = new JPanel();
		JPanel controlPanel = new JPanel();
		controlPanel.setMinimumSize(new Dimension(0, 0)); // So split pane can hide controls
		JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT,
				viewerPanel, controlPanel);
		splitPane.setContinuousLayout(true); // Optimistic!
		splitPane.setResizeWeight(1.0); // Controls' size stays fixed
		parent.add(splitPane);
        // Slice widget
		viewerPanel.setLayout(new BoxLayout(viewerPanel, BoxLayout.Y_AXIS));
        sliceViewer.setPreferredSize( new Dimension( 800, 700 ) );
        viewerPanel.add(sliceViewer);
        // Controls
		controlPanel.setLayout(new BoxLayout(controlPanel, BoxLayout.Y_AXIS));
		Container upperControls = new JPanel();
		upperControls.setLayout(new BoxLayout(upperControls, BoxLayout.X_AXIS));
		controlPanel.add(upperControls);
		// sliders
		Container slidersPanel = new JPanel();
		slidersPanel.setLayout(new BoxLayout(slidersPanel, BoxLayout.X_AXIS));
		upperControls.add(slidersPanel);
		JPanel zoomPanel = new JPanel();
		zoomPanel.setLayout(new BorderLayout(0, 0));
		slidersPanel.add(zoomPanel);
		// put a border to suggest that the zoom buttons belong with the slider
		zoomPanel.setBorder(BorderFactory.createEtchedBorder());
		zoomPanel.add(new ToolButton(zoomInAction), BorderLayout.NORTH);
		JSlider zoomSlider = new JSlider(JSlider.VERTICAL);
		zoomPanel.add(zoomSlider, BorderLayout.CENTER);
		zoomPanel.add(new ToolButton(zoomOutAction), BorderLayout.SOUTH);
		slidersPanel.add(Box.createHorizontalGlue());
		// buttons
		Container buttonsPanel = new JPanel();
		buttonsPanel.setLayout(new BoxLayout(buttonsPanel, BoxLayout.Y_AXIS));
		upperControls.add(buttonsPanel);
		buttonsPanel.add(new JButton("Fit to Window"));
		buttonsPanel.add(new JButton("Zoom Max"));
		buttonsPanel.add(new JButton("Reset View"));
		buttonsPanel.add(Box.createVerticalGlue());
		// colors
		JPanel colorsPanel = new JPanel();
		colorsPanel.setBorder(new TitledBorder("Color Channels"));
		controlPanel.add(colorsPanel);
        //Display the window.
        pack();
        setSize( getContentPane().getPreferredSize() );
        setLocation(100, 100); // TODO persist latest geometry
        setVisible(true);
	}
	
	protected void createMenus() {
		JMenuBar menuBar = new JMenuBar();
		JMenu menu, submenu;
		JMenuItem item;

		menu = new JMenu("File");
		item = new JMenuItem("Open Folder...");
		menu.add(item);
		submenu = new JMenu("Open Recent");
		submenu.setEnabled(false); // until we find some recent items...
		menu.add(submenu);
		menuBar.add(menu);

		menu = new JMenu("Edit");
		menu.add(new UndoAction());
		menu.add(new RedoAction());
		menuBar.add(menu);

		menu = new JMenu("View");
		submenu = new JMenu("Mouse Mode");
		submenu.setIcon(Icons.getIcon("mouse_left.png"));
		// only one mouse mode is active at a time
		ButtonGroup group = new ButtonGroup();
		item = new JRadioButtonMenuItem(panModeAction);
		group.add(item);
		submenu.add(item);
		item.setSelected(true);
		item = new JRadioButtonMenuItem(zoomMouseModeAction);
		group.add(item);
		submenu.add(item);
		menu.add(submenu);
		submenu = new JMenu("Scroll Mode");
		submenu.setIcon(Icons.getIcon("mouse_scroll.png"));		
		group = new ButtonGroup();
		item = new JRadioButtonMenuItem(zoomScrollModeAction);
		group.add(item);
		submenu.add(item);
		item.setSelected(true);
		menu.add(submenu);
		menu.addSeparator();
		menu.add(zoomOutAction);
		menu.add(zoomInAction);
		menuBar.add(menu);

		menu = new JMenu("Help");
		menuBar.add(menu);

		setJMenuBar(menuBar);
	}
	
	protected JComponent createStatusBar(Container parent) {
		// http://stackoverflow.com/questions/3035880/how-can-i-create-a-bar-in-the-bottom-of-a-java-app-like-a-status-bar
		JPanel statusPanel = new JPanel();
		statusPanel.setPreferredSize(new Dimension(parent.getWidth(), 20));
		statusPanel.setLayout(new BoxLayout(statusPanel, BoxLayout.X_AXIS));
		JLabel statusLabel = new JLabel("");
		statusLabel.setHorizontalAlignment(SwingConstants.LEFT);
		statusPanel.add(statusLabel);
		return statusPanel;
	}

	protected JComponent createToolBar() {
		JToolBar toolBar = new JToolBar();

		JLabel mouseModeLabel = new ToolBarIcon("mouse_left.png");
		mouseModeLabel.setToolTipText("Mouse mode:");
		toolBar.add(mouseModeLabel);
		ButtonGroup group = new ButtonGroup();
		ToolModeButton button = new ToolModeButton(panModeAction);
		group.add(button);
		toolBar.add(button);
		button.setSelected(true);
		button = new ToolModeButton(zoomMouseModeAction);
		group.add(button);
		toolBar.add(button);
		
		toolBar.addSeparator();

		mouseModeLabel = new ToolBarIcon("mouse_scroll.png");
		mouseModeLabel.setToolTipText("Scroll wheel mode:");
		toolBar.add(mouseModeLabel);
		group = new ButtonGroup();
		button = new ToolModeButton(zoomScrollModeAction);
		group.add(button);
		toolBar.add(button);
		button.setSelected(true);

		toolBar.addSeparator();
		
		return toolBar;
	}
	
}

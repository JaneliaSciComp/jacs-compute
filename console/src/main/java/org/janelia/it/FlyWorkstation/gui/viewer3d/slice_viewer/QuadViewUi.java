package org.janelia.it.FlyWorkstation.gui.viewer3d.slice_viewer;

import org.janelia.it.FlyWorkstation.gui.viewer3d.Vec3;
import org.janelia.it.FlyWorkstation.gui.viewer3d.camera.BasicObservableCamera3d;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.border.EtchedBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;
import java.net.URL;

/** 
 * Main window for QuadView application.
 * Maintained using Google WindowBuilder design tool.
 * 
 * @author Christopher M. Bruns
 *
 */
public class QuadViewUi extends JPanel
{
	private static final long serialVersionUID = 1L;

//	static {
//		// Use top menu bar on Mac
//		if (System.getProperty("os.name").contains("Mac")) {
//			  System.setProperty("apple.laf.useScreenMenuBar", "true");
//			  System.setProperty("com.apple.mrj.application.apple.menu.about.name", "QuadView");
//		}
//		try {
//			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
//		} catch (Exception e) {
//			System.out.println("Warning: Failed to set native look and feel.");
//		}
//	}

	// One shared camera for all viewers.
	// (there's only one viewer now actually, but you know...)
	private BasicObservableCamera3d camera = new BasicObservableCamera3d();
	private SliceViewer sliceViewer = new SliceViewer();
	private boolean modifierKeyPressed = false;
	private JPanel zScanPanel = new JPanel();
	private JSlider zScanSlider = new JSlider();
	private JSpinner zScanSpinner = new JSpinner();
	private JSlider zoomSlider = new JSlider(SwingConstants.VERTICAL, 0, 1000, 500);
	
	private JPanel colorPanel = new JPanel();
	private JPanel colorLockPanel = new JPanel();
    private JMenuBar menuBar = new JMenuBar();
    private JPanel toolBarPanel = new JPanel();
	private JSplitPane splitPane = new JSplitPane();
	private ColorChannelWidget colorChannelWidget_0 = new ColorChannelWidget(0, sliceViewer.getImageColorModel());
	private ColorChannelWidget colorChannelWidget_1 = new ColorChannelWidget(1, sliceViewer.getImageColorModel());
	private ColorChannelWidget colorChannelWidget_2 = new ColorChannelWidget(2, sliceViewer.getImageColorModel());
	private ColorChannelWidget colorChannelWidget_3 = new ColorChannelWidget(3, sliceViewer.getImageColorModel());
	private final ColorChannelWidget colorWidgets[]  = {
		colorChannelWidget_0, 
		colorChannelWidget_1, 
		colorChannelWidget_2, 
		colorChannelWidget_3
	};
	
	// Actions
	private final Action openFolderAction = new OpenFolderAction(sliceViewer, sliceViewer);
	private RecentFileList recentFileList;
	private final Action resetViewAction = new ResetViewAction(sliceViewer);
	private final Action resetColorsAction = new ResetColorsAction(sliceViewer.getImageColorModel());
	// mode actions (and groups)
	private final Action zoomMouseModeAction = new ZoomMouseModeAction(sliceViewer);
	private final Action panModeAction = new PanModeAction(sliceViewer);
	private final ButtonGroup mouseModeGroup = new ButtonGroup();
	private final Action zScanScrollModeAction = new ZScanScrollModeAction(sliceViewer, sliceViewer);
	private final Action zoomScrollModeAction = new ZoomScrollModeAction(sliceViewer);
	private final ButtonGroup scrollModeGroup = new ButtonGroup();
	// zoom actions
	private final Action zoomInAction = new ZoomInAction(camera);
	private final Action zoomOutAction = new ZoomOutAction(camera);
	private final Action zoomMaxAction = new ZoomMaxAction(camera, sliceViewer);
	private final Action resetZoomAction = new ResetZoomAction(sliceViewer);
	// Z scan actions
	private final Action nextZSliceAction = new NextZSliceAction(sliceViewer, sliceViewer);
	private final Action previousZSliceAction = new PreviousZSliceAction(sliceViewer, sliceViewer);
	private final Action advanceZSlicesAction = new AdvanceZSlicesAction(sliceViewer, sliceViewer, 10);
	private final Action goBackZSlicesAction = new GoBackZSlicesAction(sliceViewer, sliceViewer, -10);

	// Slots
	protected Slot1<Vec3> changeZ = new Slot1<Vec3>() {
		@Override
		public void execute(Vec3 focus) {
			int z = (int)Math.round(focus.getZ() / sliceViewer.getZResolution());
			zScanSlider.setValue(z);
			zScanSpinner.setValue(z);
		}
	};

	protected Slot1<Double> changeZoom = new Slot1<Double>() {
		@Override
		public void execute(Double zoom) {
			double zoomMin = Math.log(sliceViewer.getMinZoom()) / Math.log(2.0);
			double zoomMax = Math.log(sliceViewer.getMaxZoom()) / Math.log(2.0);
			double zoomLog = Math.log(zoom) / Math.log(2.0);
			double relativeZoom = (zoomLog - zoomMin) / (zoomMax - zoomMin);
			int sliderValue = (int)Math.round(relativeZoom * 1000.0);
			zoomSlider.setValue(sliderValue);
		}
	};
	
	protected Slot1<URL> rememberLoadedFileSlot = new Slot1<URL>() {
		@Override
		public void execute(URL url) {
			if (recentFileList == null)
				return;
			recentFileList.add(url);
		}
	};
	
	protected Slot updateRangesSlot = new Slot() {
		@Override
		public void execute() 
		{
			// Z range
			double zMin = sliceViewer.getBoundingBox3d().getMin().getZ();
			double zMax = sliceViewer.getBoundingBox3d().getMax().getZ();
			int z0 = (int)Math.round(zMin / sliceViewer.getZResolution());
			int z1 = (int)Math.round(zMax / sliceViewer.getZResolution()) - 1;
			if (z0 > z1)
				z1 = z0;
			// Z-scan is only relevant if there is more than one slice.
			boolean useZScan = ((z1 - z0) > 1);
			if (useZScan) {
				zScanPanel.setVisible(true);
				sliceViewer.setWheelMode(new ZScanMode(sliceViewer));
				zScanScrollModeAction.setEnabled(true);
				zScanScrollModeAction.actionPerformed(new ActionEvent(this, 0, ""));
				int z = (int)Math.round(sliceViewer.getFocus().getZ() / sliceViewer.getZResolution());
				if (z < z0)
					z = z0;
				if (z > z1)
					z = z1;
				zScanSlider.setMinimum(z0);
				zScanSlider.setMaximum(z1);
				zScanSlider.setValue(z);
				zScanSpinner.setModel(new SpinnerNumberModel(z, z0, z1, 1));
			}
			else { // no Z scan
				zScanPanel.setVisible(false);
				zoomScrollModeAction.actionPerformed(new ActionEvent(this, 0, ""));
				zScanScrollModeAction.setEnabled(false);
			}
		}
		// TODO update zoom range too?
	};

    /**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
                    JFrame mainFrame = new JFrame();
                    mainFrame.setTitle("QuadView");
                    mainFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
                    mainFrame.setResizable(true);
                    mainFrame.setBounds(100, 100, 994, 653);
                    QuadViewUi contentPane = new QuadViewUi(mainFrame, true);
                    mainFrame.setContentPane(contentPane);

                    mainFrame.setVisible(true);
                } catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 */
	public QuadViewUi(JFrame parentFrame, boolean overrideFrameMenuBar)
	{
		colorChannelWidget_3.setVisible(false);
		colorChannelWidget_2.setVisible(false);
		colorChannelWidget_1.setVisible(false);
		colorChannelWidget_0.setVisible(false);
		setupUi(parentFrame, overrideFrameMenuBar);
        interceptModifierKeyPresses();
	}

	private void setupUi(JFrame parentFrame, boolean overrideFrameMenuBar) {
        setBounds(100, 100, 994, 653);
        setBorder(new EmptyBorder(5, 5, 5, 5));
		setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));

		JPanel glassPane = new HudPanel();
		parentFrame.setGlassPane(glassPane);
		// glassPane.setVisible(true);
        setupMenu(parentFrame, overrideFrameMenuBar);
        JPanel toolBarPanel = setupToolBar();
		
		// JSplitPane splitPane = new JSplitPane();
		splitPane.setResizeWeight(1.00);
		splitPane.setOrientation(JSplitPane.VERTICAL_SPLIT);
		toolBarPanel.add(splitPane, BorderLayout.CENTER);
		
		// JPanel colorPanel = new JPanel();
		splitPane.setRightComponent(colorPanel);
		colorPanel.setLayout(new BoxLayout(colorPanel, BoxLayout.Y_AXIS));
		
		// Avoid loop to add color widgets, so WindowBuilder can parse this.
		colorPanel.add(colorWidgets[0]);
		colorPanel.add(colorWidgets[1]);
		colorPanel.add(colorWidgets[2]);
		colorPanel.add(colorWidgets[3]);
		
		// JPanel colorLockPanel = new JPanel();
		colorLockPanel.setVisible(false);
		colorPanel.add(colorLockPanel);
		colorLockPanel.setLayout(new BoxLayout(colorLockPanel, BoxLayout.X_AXIS));
		
		colorLockPanel.add(Box.createHorizontalStrut(30));
		
		JToggleButton lockBlackButton = new JToggleButton("");
		lockBlackButton.setToolTipText("Synchronize channel black levels");
		lockBlackButton.setMargin(new Insets(0, 0, 0, 0));
		lockBlackButton.setRolloverIcon(new ImageIcon(QuadViewUi.class.getResource("/images/lock_unlock.png")));
		lockBlackButton.setRolloverSelectedIcon(new ImageIcon(QuadViewUi.class.getResource("/images/lock.png")));
		lockBlackButton.setIcon(new ImageIcon(QuadViewUi.class.getResource("/images/lock_unlock.png")));
		lockBlackButton.setSelectedIcon(new ImageIcon(QuadViewUi.class.getResource("/images/lock.png")));
		lockBlackButton.setSelected(true);
		colorLockPanel.add(lockBlackButton);
		lockBlackButton.addChangeListener(new ChangeListener() {
			@Override
			public void stateChanged(ChangeEvent event) {
				ImageColorModel colorModel = sliceViewer.getImageColorModel();
				if (colorModel == null)
					return;
				AbstractButton button = (AbstractButton)event.getSource();
				colorModel.setBlackSynchronized(button.isSelected());
			}
		});
		
		colorLockPanel.add(Box.createHorizontalGlue());

		JToggleButton lockGrayButton = new JToggleButton("");
		lockGrayButton.setToolTipText("Synchronize channel gray levels");
		lockGrayButton.setMargin(new Insets(0, 0, 0, 0));
		lockGrayButton.setRolloverIcon(new ImageIcon(QuadViewUi.class.getResource("/images/lock_unlock.png")));
		lockGrayButton.setRolloverSelectedIcon(new ImageIcon(QuadViewUi.class.getResource("/images/lock.png")));
		lockGrayButton.setIcon(new ImageIcon(QuadViewUi.class.getResource("/images/lock_unlock.png")));
		lockGrayButton.setSelectedIcon(new ImageIcon(QuadViewUi.class.getResource("/images/lock.png")));
		lockGrayButton.setSelected(true);
		colorLockPanel.add(lockGrayButton);
		lockGrayButton.addChangeListener(new ChangeListener() {
			@Override
			public void stateChanged(ChangeEvent event) {
				ImageColorModel colorModel = sliceViewer.getImageColorModel();
				if (colorModel == null)
					return;
				AbstractButton button = (AbstractButton)event.getSource();
				colorModel.setGammaSynchronized(button.isSelected());
			}
		});
		
		colorLockPanel.add(Box.createHorizontalGlue());

		JToggleButton lockWhiteButton = new JToggleButton("");
		lockWhiteButton.setToolTipText("Synchronize channel white levels");
		lockWhiteButton.setMargin(new Insets(0, 0, 0, 0));
		lockWhiteButton.setRolloverIcon(new ImageIcon(QuadViewUi.class.getResource("/images/lock_unlock.png")));
		lockWhiteButton.setRolloverSelectedIcon(new ImageIcon(QuadViewUi.class.getResource("/images/lock.png")));
		lockWhiteButton.setIcon(new ImageIcon(QuadViewUi.class.getResource("/images/lock_unlock.png")));
		lockWhiteButton.setSelectedIcon(new ImageIcon(QuadViewUi.class.getResource("/images/lock.png")));
		lockWhiteButton.setSelected(true);
		colorLockPanel.add(lockWhiteButton);
		lockWhiteButton.addChangeListener(new ChangeListener() {
			@Override
			public void stateChanged(ChangeEvent event) {
				ImageColorModel colorModel = sliceViewer.getImageColorModel();
				if (colorModel == null)
					return;
				AbstractButton button = (AbstractButton)event.getSource();
				colorModel.setWhiteSynchronized(button.isSelected());
			}
		});
		
		colorLockPanel.add(Box.createHorizontalStrut(30));

		sliceViewer.getImageColorModel().getColorModelInitializedSignal().connect(new Slot() {
			@Override
			public void execute() {
				// System.out.println("Updating slider visibility");
				int sc = sliceViewer.getImageColorModel().getChannelCount();
				colorPanel.setVisible(sc > 0);
				int c = 0;
				for (ColorChannelWidget w : colorWidgets) {
					w.setVisible(c < sc);
					c += 1;
				}
				colorLockPanel.setVisible(sc > 1);
				splitPane.resetToPreferredSizes();
				// TODO Trying without success to get sliders to initially paint correctly
				colorPanel.validate();
				colorPanel.repaint();
			}
		});
		
		JSplitPane splitPane_1 = new JSplitPane();
		splitPane_1.setResizeWeight(1.00);
		splitPane.setLeftComponent(splitPane_1);

		JPanel viewerPanel = new JPanel();
		splitPane_1.setLeftComponent(viewerPanel);
		viewerPanel.setLayout(new BoxLayout(viewerPanel, BoxLayout.Y_AXIS));
		
		// SliceViewer sliceViewer = new SliceViewer();
		sliceViewer.setCamera(camera);
		sliceViewer.setBackground(Color.DARK_GRAY);
		viewerPanel.add(sliceViewer);
        sliceViewer.getDataChangedSignal().connect(updateRangesSlot);
		sliceViewer.getZoomChangedSignal().connect(changeZoom);
        sliceViewer.getCamera().getFocusChangedSignal().connect(changeZ);
		sliceViewer.getFileLoadedSignal().connect(rememberLoadedFileSlot);
		
		// JPanel zScanPanel = new JPanel();
		viewerPanel.add(zScanPanel);
		zScanPanel.setLayout(new BoxLayout(zScanPanel, BoxLayout.X_AXIS));
	
        ToolButton button_2 = new ToolButton(goBackZSlicesAction);
		button_2.setAction(goBackZSlicesAction);
		button_2.setMargin(new Insets(0, 0, 0, 0));
		button_2.setHideActionText(true);
		button_2.setAlignmentX(0.5f);
		zScanPanel.add(button_2);
		
		ToolButton button_1 = new ToolButton(previousZSliceAction);
		button_1.setAction(previousZSliceAction);
		button_1.setMargin(new Insets(0, 0, 0, 0));
		button_1.setHideActionText(true);
		button_1.setAlignmentX(0.5f);
		zScanPanel.add(button_1);
		zScanSlider.addChangeListener(new ChangeListener() {
			public void stateChanged(ChangeEvent arg0) {
				setZSlice(zScanSlider.getValue());
			}
		});
		
		// JSlider zScanSlider = new JSlider();
		zScanSlider.setPreferredSize(new Dimension(32767, 29));
		zScanSlider.setMajorTickSpacing(10);
		zScanSlider.setPaintTicks(true);
		zScanPanel.add(zScanSlider);
		
		ToolButton button_3 = new ToolButton(nextZSliceAction);
		button_3.setAction(nextZSliceAction);
		button_3.setMargin(new Insets(0, 0, 0, 0));
		button_3.setHideActionText(true);
		button_3.setAlignmentX(0.5f);
		zScanPanel.add(button_3);
		
		ToolButton button_4 = new ToolButton(advanceZSlicesAction);
		button_4.setAction(advanceZSlicesAction);
		button_4.setMargin(new Insets(0, 0, 0, 0));
		button_4.setHideActionText(true);
		button_4.setAlignmentX(0.5f);
		zScanPanel.add(button_4);
		zScanSpinner.addChangeListener(new ChangeListener() {
			public void stateChanged(ChangeEvent arg0) {
				setZSlice((Integer)zScanSpinner.getValue());
			}
		});
		
		// JSpinner zScanSpinner = new JSpinner();
		zScanSpinner.setPreferredSize(new Dimension(75, 28));
		zScanSpinner.setMaximumSize(new Dimension(120, 28));
		zScanSpinner.setMinimumSize(new Dimension(65, 28));
		zScanPanel.add(zScanSpinner);
		
		JPanel controlsPanel = new JPanel();
		splitPane_1.setRightComponent(controlsPanel);
		controlsPanel.setLayout(new BoxLayout(controlsPanel, BoxLayout.X_AXIS));
		
		JPanel panel_1 = new JPanel();
		panel_1.setBorder(new EtchedBorder(EtchedBorder.LOWERED, null, null));
		controlsPanel.add(panel_1);
		panel_1.setLayout(new BoxLayout(panel_1, BoxLayout.Y_AXIS));
		
		ToolButton btnNewButton_2 = new ToolButton(zoomInAction);
		btnNewButton_2.setAlignmentX(0.5f);
		btnNewButton_2.setMargin(new Insets(0, 0, 0, 0));
		btnNewButton_2.setHideActionText(true);
		btnNewButton_2.setAction(zoomInAction);
		panel_1.add(btnNewButton_2);
		
		// JSlider zoomSlider = new JSlider();
		zoomSlider.setOrientation(SwingConstants.VERTICAL);
		zoomSlider.setMaximum(1000);
		// Kludge to get decent vertical JSlider on Windows
		zoomSlider.setPaintTicks(true);
		zoomSlider.setMajorTickSpacing(1000);
		//
		panel_1.add(zoomSlider);
		zoomSlider.addChangeListener(new ChangeListener() {
			public void stateChanged(ChangeEvent arg0) {
				int value = zoomSlider.getValue();
				double relativeZoom = value / 1000.0;
				// log scale
				double zoomMin = Math.log(sliceViewer.getMinZoom()) / Math.log(2.0);
				double zoomMax = Math.log(sliceViewer.getMaxZoom()) / Math.log(2.0);
				double zoom = zoomMin + relativeZoom * (zoomMax - zoomMin);
				zoom = Math.pow(2.0, zoom);
				sliceViewer.setPixelsPerSceneUnit(zoom);
			}
		});
		
		ToolButton button = new ToolButton(zoomOutAction);
		button.setAction(zoomOutAction);
		button.setMargin(new Insets(0, 0, 0, 0));
		button.setHideActionText(true);
		button.setAlignmentX(0.5f);
		panel_1.add(button);
		
		JPanel buttonsPanel = new JPanel();
		controlsPanel.add(buttonsPanel);
		buttonsPanel.setLayout(new BoxLayout(buttonsPanel, BoxLayout.Y_AXIS));
		
		JButton btnNewButton_1 = new JButton("New button");
		btnNewButton_1.setAction(resetZoomAction);
		buttonsPanel.add(btnNewButton_1);
		
		JButton btnNewButton = new JButton("New button");
		btnNewButton.setAction(zoomMaxAction);
		buttonsPanel.add(btnNewButton);
		
		JButton resetViewButton = new JButton("New button");
		resetViewButton.setAction(resetViewAction);
		buttonsPanel.add(resetViewButton);
		
		Component verticalGlue = Box.createVerticalGlue();
		buttonsPanel.add(verticalGlue);
		
		JButton btnNewButton_3 = new JButton("New button");
		btnNewButton_3.setAction(resetColorsAction);
		buttonsPanel.add(btnNewButton_3);
		
		JPanel statusBar = new JPanel();
		statusBar.setMaximumSize(new Dimension(32767, 30));
		statusBar.setMinimumSize(new Dimension(10, 30));
		add(statusBar);
		statusBar.setLayout(new BoxLayout(statusBar, BoxLayout.X_AXIS));
		
		JLabel lblNewLabel = new JLabel("status area");
		statusBar.add(lblNewLabel);
	}

	private void interceptModifierKeyPresses() 
	{ 
        // Intercept Shift key strokes at the highest level JComponent we can find.
        InputMap inputMap = getInputMap(JComponent.WHEN_ANCESTOR_OF_FOCUSED_COMPONENT);
        inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_SHIFT, KeyEvent.SHIFT_DOWN_MASK, false),
        		"ModifierPressed");
        inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_CONTROL, KeyEvent.CTRL_DOWN_MASK, false),
        		"ModifierPressed");
        
        inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_CONTROL, 0, true),
				"ModifierReleased");
        inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_SHIFT, 0, true),
				"ModifierReleased");

        ActionMap actionMap = getActionMap();
        actionMap.put("ModifierPressed", new AbstractAction() 
        {
			private static final long serialVersionUID = 1L;
			@Override
            public void actionPerformed(ActionEvent e) {
                setModifierKeyPressed(true);
            }
        });
        actionMap.put("ModifierReleased", new AbstractAction() 
        {
			private static final long serialVersionUID = 1L;
			@Override
            public void actionPerformed(ActionEvent e) {
                setModifierKeyPressed(false);
            }
        });
	}

	private JPanel setupToolBar() {
		add(toolBarPanel);
		toolBarPanel.setLayout(new BorderLayout(0, 0));
		
		JToolBar toolBar = new JToolBar();
		toolBarPanel.add(toolBar, BorderLayout.NORTH);
		
		JLabel lblNewLabel_1 = new JLabel("");
		lblNewLabel_1.setToolTipText("Mouse Mode:");
		lblNewLabel_1.setFocusable(false);
		lblNewLabel_1.setIcon(new ImageIcon(QuadViewUi.class.getResource("/images/mouse_left.png")));
		toolBar.add(lblNewLabel_1);
		
		JToggleButton tglBtnPanMode = new JToggleButton("");
		mouseModeGroup.add(tglBtnPanMode);
		tglBtnPanMode.setSelected(true);
		tglBtnPanMode.setAction(panModeAction);
		tglBtnPanMode.setMargin(new Insets(0, 0, 0, 0));
		tglBtnPanMode.setHideActionText(true);
		tglBtnPanMode.setFocusable(false);
		toolBar.add(tglBtnPanMode);
		
		JToggleButton tglbtnZoomMouseMode = new JToggleButton("");
		mouseModeGroup.add(tglbtnZoomMouseMode);
		tglbtnZoomMouseMode.setMargin(new Insets(0, 0, 0, 0));
		tglbtnZoomMouseMode.setFocusable(false);
		tglbtnZoomMouseMode.setHideActionText(true);
		tglbtnZoomMouseMode.setAction(zoomMouseModeAction);
		toolBar.add(tglbtnZoomMouseMode);

		toolBar.addSeparator();
		
		JLabel scrollModeLabel = new JLabel("");
		scrollModeLabel.setIcon(new ImageIcon(QuadViewUi.class.getResource("/images/mouse_scroll.png")));
		scrollModeLabel.setFocusable(false);
		toolBar.add(scrollModeLabel);
		
		JToggleButton toggleButton = new JToggleButton("");
		scrollModeGroup.add(toggleButton);
		toggleButton.setSelected(true);
		toggleButton.setAction(zScanScrollModeAction);
		toggleButton.setMargin(new Insets(0, 0, 0, 0));
		toggleButton.setHideActionText(true);
		toggleButton.setFocusable(false);
		toolBar.add(toggleButton);
		
		JToggleButton toggleButton_1 = new JToggleButton("");
		scrollModeGroup.add(toggleButton_1);
		toggleButton_1.setAction(zoomScrollModeAction);
		toggleButton_1.setMargin(new Insets(0, 0, 0, 0));
		toggleButton_1.setHideActionText(true);
		toggleButton_1.setFocusable(false);
		toolBar.add(toggleButton_1);
		
		toolBar.addSeparator();
		return toolBarPanel;
	}

	private void setModifierKeyPressed(boolean pressed) 
	{
		// Has the status changed since last time?
		if (pressed == modifierKeyPressed)
			return; // no change
		modifierKeyPressed = pressed; // changed!
		// Shift to select zoom scroll mode
		if (pressed)
			zoomScrollModeAction.actionPerformed(new ActionEvent(this, 0, ""));
		else if (zScanScrollModeAction.isEnabled())
			zScanScrollModeAction.actionPerformed(new ActionEvent(this, 0, ""));
	}
	
	private boolean setZSlice(int z) {
		Vec3 oldFocus = sliceViewer.getFocus();
		int oldValue = (int)Math.round(oldFocus.getZ() / sliceViewer.getZResolution());
		if (oldValue == z)
			return false; // camera is already pretty close
		double newZ = z * sliceViewer.getZResolution();
		double minZ = sliceViewer.getBoundingBox3d().getMin().getZ();
		double maxZ = sliceViewer.getBoundingBox3d().getMax().getZ();
		newZ = Math.max(newZ, minZ);
		newZ = Math.min(newZ, maxZ);
		sliceViewer.setFocus(new Vec3(oldFocus.getX(), oldFocus.getY(), newZ));
		return true;
	}

    private void setupMenu(JFrame parentFrame, boolean overrideFrameMenuBar) {
        if (overrideFrameMenuBar) {
            parentFrame.setJMenuBar(menuBar);
            addFileMenuItem();
            addEditMenuItem();
            addViewMenuItem();
            addHelpMenuItem();
        }
        else {
            toolBarPanel.add(addViewMenuItem());
//            JPanel tempPanel = new JPanel();
//            tempPanel.setLayout(new BoxLayout(tempPanel, BoxLayout.LINE_AXIS));
//            tempPanel.add(menuBar);
//            tempPanel.add(Box.createHorizontalGlue());
//            add(tempPanel);
        }
    }

    private JMenuItem addViewMenuItem() {
        JMenu mnView = new JMenu("View");
        menuBar.add(mnView);

        JMenu mnMouseMode = new JMenu("Mouse Mode");
        mnMouseMode.setIcon(new ImageIcon(QuadViewUi.class.getResource("/images/mouse_left.png")));
        mnView.add(mnMouseMode);

        JRadioButtonMenuItem panModeItem = new JRadioButtonMenuItem("New radio item");
        panModeItem.setSelected(true);
        panModeItem.setAction(panModeAction);
        mnMouseMode.add(panModeItem);

        JRadioButtonMenuItem zoomMouseModeItem = new JRadioButtonMenuItem("New radio item");
        zoomMouseModeItem.setAction(zoomMouseModeAction);
        mnMouseMode.add(zoomMouseModeItem);

        JMenu mnScrollMode = new JMenu("Scroll Mode");
        mnScrollMode.setIcon(new ImageIcon(QuadViewUi.class.getResource("/images/mouse_scroll.png")));
        mnView.add(mnScrollMode);

        JRadioButtonMenuItem rdbtnmntmNewRadioItem = new JRadioButtonMenuItem("New radio item");
        rdbtnmntmNewRadioItem.setSelected(true);
        rdbtnmntmNewRadioItem.setAction(zScanScrollModeAction);
        mnScrollMode.add(rdbtnmntmNewRadioItem);

        JRadioButtonMenuItem mntmNewMenuItem_2 = new JRadioButtonMenuItem("New menu item");
        mntmNewMenuItem_2.setAction(zoomScrollModeAction);
        mnScrollMode.add(mntmNewMenuItem_2);

        JSeparator separator = new JSeparator();
        mnView.add(separator);

        JMenu mnZoom = new JMenu("Zoom");
        mnView.add(mnZoom);

        mnZoom.add(resetZoomAction);
        mnZoom.add(zoomOutAction);
        mnZoom.add(zoomInAction);
        mnZoom.add(zoomMaxAction);

        JSeparator separator_1 = new JSeparator();
        mnView.add(separator_1);

        JMenu mnZScan = new JMenu("Z Scan");
        mnView.add(mnZScan);

        JMenuItem mntmNewMenuItem = new JMenuItem("New menu item");
        mntmNewMenuItem.setAction(goBackZSlicesAction);
        mnZScan.add(mntmNewMenuItem);

        JMenuItem menuItem_2 = new JMenuItem("New menu item");
        menuItem_2.setAction(previousZSliceAction);
        mnZScan.add(menuItem_2);

        JMenuItem menuItem_1 = new JMenuItem("New menu item");
        menuItem_1.setAction(nextZSliceAction);
        mnZScan.add(menuItem_1);

        JMenuItem menuItem = new JMenuItem("New menu item");
        menuItem.setAction(advanceZSlicesAction);
        mnZScan.add(menuItem);

        JSeparator separator_2 = new JSeparator();
        mnView.add(separator_2);

        JMenuItem mntmNewMenuItem_3 = new JMenuItem("New menu item");
        mntmNewMenuItem_3.setAction(resetColorsAction);
        mnView.add(mntmNewMenuItem_3);
        return mnView;
    }

    public JMenuItem addFileMenuItem() {
        JMenu mnFile = new JMenu("File");
        menuBar.add(mnFile);

        JMenuItem mntmNewMenuItem_1 = new JMenuItem("New menu item");
        mntmNewMenuItem_1.setAction(openFolderAction);
        mnFile.add(mntmNewMenuItem_1);

        JMenu mnNewMenu = new JMenu("Open Recent");
        mnNewMenu.setVisible(false);
        mnFile.add(mnNewMenu);
        recentFileList = new RecentFileList(mnNewMenu);
        sliceViewer.getFileLoadedSignal().connect(rememberLoadedFileSlot);
        recentFileList.getOpenUrlRequestedSignal().connect(sliceViewer.getLoadUrlSlot());
        return mnFile;
    }

    public JMenuItem addEditMenuItem() {
        JMenu mnEdit = new JMenu("Edit");
        menuBar.add(mnEdit);
        return mnEdit;
    }

    public JMenuItem addHelpMenuItem() {
        JMenu mnHelp = new JMenu("Help");
        menuBar.add(mnHelp);
        return mnHelp;
    }
}

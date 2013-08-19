package org.janelia.it.FlyWorkstation.gui.viewer3d.demo;

import java.awt.Dimension;
import javax.media.opengl.awt.GLJPanel;
import javax.swing.JFrame;

import org.janelia.it.FlyWorkstation.gui.viewer3d.TeapotActor;
import org.janelia.it.FlyWorkstation.gui.viewer3d.Vec3;
import org.janelia.it.FlyWorkstation.gui.viewer3d.camera.BasicObservableCamera3d;
import org.janelia.it.FlyWorkstation.gui.viewer3d.camera.ObservableCamera3d;
import org.janelia.it.FlyWorkstation.gui.viewer3d.slice_viewer.Slot;

@SuppressWarnings("serial")
public class TeapotDemo extends JFrame
{

	public static void main(String[] args) {
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
            	new TeapotDemo();
            }
        });
	}

	
	private GLJPanel glPanel = new GLJPanel();
	
	public TeapotDemo() {
    	setTitle("Teapot Demo");
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        // Create canvas for openGL display of teapot
        glPanel.setPreferredSize(new Dimension(1280, 800));
        getContentPane().add(glPanel);

        // Watch keyboard to detect fullscreen shortcut.
        // Hey! A one line full screen mode decorator!
        addKeyListener(new FullScreenMode(this));

        // Create non-stereo-3D actor component
        CompositeGLActor monoActor = new CompositeGLActor();
        // Use 3D lighting
        monoActor.addActor(new LightingActor());
        // Prepare to draw a teapot
		monoActor.addActor(new TeapotActor());
        
		// Create camera
		ObservableCamera3d camera = new BasicObservableCamera3d();
		camera.setFocus(new Vec3(0, 0, 0));
		camera.setPixelsPerSceneUnit(200);

		// Wrap mono actor in stereo 3D mode
        AbstractStereoMode stereoMode = new LeftRightStereoMode(
        		camera, monoActor);
        // so I can look at it cross-eyed for testing
        // stereoMode.setSwapEyes(true);
        glPanel.addGLEventListener(stereoMode);
        stereoMode.viewChangedSignal.connect(
        		new Slot() {
					@Override
					public void execute() {
						glPanel.repaint();
					}
        		});
        
        // Apply mouse interactions: drag to rotate etc.
        // Another one-line functionality decorator!
        new TrackballInteractor(glPanel, camera);        
		
        //Display the window.
        pack();
        setVisible(true);		
	};
	
}

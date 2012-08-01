package org.janelia.it.FlyWorkstation.gui.framework.console;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JDialog;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.KeyStroke;

import org.janelia.it.FlyWorkstation.gui.dialogs.DataGroupDialog;
import org.janelia.it.FlyWorkstation.gui.dialogs.ScreenEvaluationDialog;
import org.janelia.it.FlyWorkstation.gui.framework.session_mgr.SessionMgr;
import org.janelia.it.FlyWorkstation.gui.util.SystemInfo;

/**
 * Created by IntelliJ IDEA.
 * User: saffordt
 * Date: 2/8/11
 * Time: 3:47 PM
 */
public class ServicesMenu extends JMenu {

	private KeyStroke screenEvalAccel = KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_R, SystemInfo.isMac?java.awt.Event.META_MASK:java.awt.Event.CTRL_MASK);
	
    public ServicesMenu(final Browser browser) {
        super("Services");

        JMenuItem dataCircleMenuItem = new JMenuItem("Data Groups Manager...");
        dataCircleMenuItem.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                JDialog dialog = new DataGroupDialog();
                dialog.setVisible(true);
            }
        });
        add(dataCircleMenuItem);
        
        JMenuItem neuronSeparationMenuItem = new JMenuItem("Neuron Separation Service...");
        neuronSeparationMenuItem.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent actionEvent) {
                SessionMgr.getSessionMgr().getActiveBrowser().getRunNeuronSeparationDialog().showDialog();
            }
        });
        add(neuronSeparationMenuItem);

        ScreenEvaluationDialog screenEvaluationDialog = browser.getScreenEvaluationDialog();

        if (screenEvaluationDialog.isAccessible()) {
        	JMenuItem screenEvaluationMenuItem = new JMenuItem("Screen Evaluation...");
            screenEvaluationMenuItem.addActionListener(new ActionListener() {
                public void actionPerformed(ActionEvent actionEvent) {
                    SessionMgr.getSessionMgr().getActiveBrowser().getScreenEvaluationDialog().showDialog();
                }
            });
            screenEvaluationMenuItem.setAccelerator(screenEvalAccel);
            
            add(screenEvaluationMenuItem);
        }
    }
}

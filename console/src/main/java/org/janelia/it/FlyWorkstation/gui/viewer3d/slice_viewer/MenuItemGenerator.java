package org.janelia.it.FlyWorkstation.gui.viewer3d.slice_viewer;

import java.awt.event.MouseEvent;
import java.util.List;

import javax.swing.JMenuItem;

public abstract class MenuItemGenerator {
    public abstract List<JMenuItem> getMenus(MouseEvent event);
}

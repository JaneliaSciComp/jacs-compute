package org.janelia.it.FlyWorkstation.gui.viewer3d.slice_viewer;

import java.util.Observable;

public abstract class QtSlot
implements QtBasicSignalSlot
{
	protected Object receiver;

	public QtSlot(Object receiver) {
		this.receiver = receiver;
	}

	// Override this execute() method for your particular slot
	public abstract void execute();

	@Override
	public void update(Observable o, Object arg) {
		execute();	
	}
}

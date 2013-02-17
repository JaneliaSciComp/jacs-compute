package org.janelia.it.FlyWorkstation.gui.viewer3d.slice_viewer;

import java.util.Observable;


// Java Observable that acts a bit like a Qt Signal
public class QtSignal
extends Observable
implements QtBasicSignalSlot
{
	public void emit() {
		setChanged();
		notifyObservers();
	}
	
	public void connect(QtBasicSignalSlot dest) {
		addObserver(dest);
	}

	@Override
	public void update(Observable o, Object arg) {
		emit();	
	}
}

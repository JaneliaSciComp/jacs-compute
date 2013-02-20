package org.janelia.it.FlyWorkstation.gui.viewer3d.camera;

import java.util.Observable;

import org.janelia.it.FlyWorkstation.gui.viewer3d.Rotation;
import org.janelia.it.FlyWorkstation.gui.viewer3d.Vec3;
import org.janelia.it.FlyWorkstation.gui.viewer3d.interfaces.Camera3d;
import org.janelia.it.FlyWorkstation.gui.viewer3d.slice_viewer.QtSignal;
import org.janelia.it.FlyWorkstation.gui.viewer3d.slice_viewer.QtSignal1;

public class BasicObservableCamera3d 
extends Observable 
implements Camera3d, ObservableCamera3d
{
	private Camera3d camera = new BasicCamera3d();
	protected QtSignal viewChanged = new QtSignal();
	protected QtSignal1<Double> zoomChanged = new QtSignal1<Double>();
	
	/* (non-Javadoc)
	 * @see org.janelia.it.FlyWorkstation.gui.viewer3d.camera.ObservableCamera3d#getViewChangedSignal()
	 */
	@Override
	public QtSignal getViewChangedSignal() {
		return viewChanged;
	}
	
	@Override
	public QtSignal1<Double> getZoomChanged() {
		return zoomChanged;
	}

	@Override
	public boolean incrementFocusPixels(double dx, double dy, double dz) {
		return markAndNotify(camera.incrementFocusPixels(dx, dy, dz));
	}

	@Override
	public boolean incrementFocusPixels(Vec3 offset) {
		return markAndNotify(camera.incrementFocusPixels(offset));
	}

	@Override
    public boolean incrementZoom(double zoomRatio) {
		boolean result = markAndNotify(camera.incrementZoom(zoomRatio));
		if (result) {
			zoomChanged.emit(camera.getPixelsPerSceneUnit());
		}
		return result;
	}
	
	private boolean markAndNotify(boolean changed) {
		if (! changed)
			return false;
		// System.out.println("emit viewChanged");
		viewChanged.emit();	
		return true;
	}

	@Override
	public Vec3 getFocus() {
		return camera.getFocus();
	}

	@Override
	public double getPixelsPerSceneUnit() {
		return camera.getPixelsPerSceneUnit();
	}

	@Override
	public Rotation getRotation() {
		return camera.getRotation();
	}

	@Override
	public boolean resetFocus() {
		return markAndNotify(camera.resetFocus());
	}

	@Override
	public boolean resetRotation() {
		return markAndNotify(camera.resetRotation());
	}

	@Override
	public boolean setFocus(Vec3 f) {
		return markAndNotify(camera.setFocus(f));
	}

	@Override
	public boolean setFocus(double x, double y, double z) {
		return markAndNotify(camera.setFocus(x, y, z));
	}

	@Override
	public boolean setRotation(Rotation r) {
		return markAndNotify(camera.setRotation(r));
	}

	@Override
	public boolean setPixelsPerSceneUnit(double pixelsPerSceneUnit) {
		boolean result = markAndNotify(camera.setPixelsPerSceneUnit(pixelsPerSceneUnit));
		if (result) {
			zoomChanged.emit(camera.getPixelsPerSceneUnit());
		}
		return result;
	}
}

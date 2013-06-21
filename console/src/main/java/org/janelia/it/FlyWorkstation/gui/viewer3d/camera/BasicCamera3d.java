package org.janelia.it.FlyWorkstation.gui.viewer3d.camera;

import org.janelia.it.FlyWorkstation.gui.viewer3d.Rotation3d;
import org.janelia.it.FlyWorkstation.gui.viewer3d.Vec3;
import org.janelia.it.FlyWorkstation.gui.viewer3d.interfaces.Camera3d;

public class BasicCamera3d
implements Camera3d
{
	// View center
    private Vec3 focus = new Vec3(0,0,0); // in scene units
    private Rotation3d rotation = new Rotation3d();
    private double pixelsPerSceneUnit = 1.0; // zoom

	public Vec3 getFocus() {
		return focus;
	}

	public double getPixelsPerSceneUnit() {
		return pixelsPerSceneUnit;
	}

	public Rotation3d getRotation() {
		return rotation;
	}
	
    @Override
	public boolean incrementFocusPixels(double dx, double dy, double dz) {
		return incrementFocusPixels(new Vec3(dx, dy, dz));
	}
	
	@Override
	public boolean incrementFocusPixels(Vec3 offset) {
		Vec3 v = offset.times(1.0 / pixelsPerSceneUnit);
		return setFocus(focus.plus(v));
	}

    public boolean incrementZoom(double zoomRatio) {
    		if (zoomRatio == 1.0)
    			return false; // no change
    		if (zoomRatio <= 0.0)
    			return false; // impossible
    		pixelsPerSceneUnit *= zoomRatio;
    		return true;
    }
    
    public boolean resetFocus() {
    		return setFocus(new Vec3(0,0,0));
    }
    
    public boolean resetRotation() {
    		return setRotation(new Rotation3d());
    }
    
    public boolean setFocus(Vec3 f) {
		if (f == focus)
			return false; // no change
		if (Double.isNaN(f.getX())) {
			System.out.println("Camera NaN");
			return false;
		}
		focus = f;
		// System.out.println(f);
		return true;
    }

	@Override
	public boolean setFocus(double x, double y, double z) {
		return setFocus(new Vec3(x, y, z));
	}

    public boolean setRotation(Rotation3d r) {
		if (r == rotation)
			return false; // no change
		rotation = r;
		return true;
    }

	public boolean setPixelsPerSceneUnit(double pixelsPerSceneUnit) {
		if (this.pixelsPerSceneUnit == pixelsPerSceneUnit)
			return false; // no change
		if (Double.isNaN(pixelsPerSceneUnit)) {
			System.out.println("Camera zoom NaN");
			return false;
		}
		this.pixelsPerSceneUnit = pixelsPerSceneUnit;
		return true;
	}

}

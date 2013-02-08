package org.janelia.it.FlyWorkstation.gui.viewer3d.slice_viewer;

import java.awt.geom.Point2D;

import javax.media.opengl.GL2;

import org.janelia.it.FlyWorkstation.gui.viewer3d.BoundingBox;
import org.janelia.it.FlyWorkstation.gui.viewer3d.GLActor;
import org.janelia.it.FlyWorkstation.gui.viewer3d.Vec3;

public class TileActor 
implements GLActor
{
	Vec3 origin = new Vec3();
	Point2D pixelSize = new Point2D.Float(1.0f, 1.0f);
	

	@Override
	public void display(GL2 gl) {
		gl.glBegin(GL2.GL_QUADS);
		gl.glEnd();
	}

	@Override
	public BoundingBox getBoundingBox() {
		return null;
	}

	@Override
	public void init(GL2 gl) {
	}

	@Override
	public void dispose(GL2 gl) {
	}
}

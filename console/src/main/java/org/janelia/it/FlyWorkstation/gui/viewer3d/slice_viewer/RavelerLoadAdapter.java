package org.janelia.it.FlyWorkstation.gui.viewer3d.slice_viewer;

import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import javax.imageio.ImageIO;
import javax.media.opengl.GL2;

import com.jogamp.opengl.util.texture.awt.AWTTextureIO;

public class RavelerLoadAdapter implements PyramidTextureLoadAdapter {

	private URL urlStalk;
	
	public RavelerLoadAdapter(URL urlStalk) {
		this.urlStalk = urlStalk;
	}
	
	protected ByteArrayInputStream downloadBytes(URL url) 
	throws IOException 
	{
		// First load bytes, THEN parse image (for more surgical timing measurements)
		// http://stackoverflow.com/questions/2295221/java-net-url-read-stream-to-byte
		// TODO - speed things up by combining download and decompress
		ByteArrayOutputStream byteStream0 = new ByteArrayOutputStream();
		byte[] chunk = new byte[32768];
		int bytesRead;
		InputStream stream = new BufferedInputStream(url.openStream());
		while ((bytesRead = stream.read(chunk)) > 0) {
			byteStream0.write(chunk, 0, bytesRead);
		}
		byte[] byteArray = byteStream0.toByteArray();
		ByteArrayInputStream byteStream = new ByteArrayInputStream(byteArray);
		return byteStream;
	}

	private BufferedImage decodeImage(ByteArrayInputStream byteStream)
	throws IOException 
	{
		BufferedImage image = ImageIO.read(byteStream);
		return image;
	}

	@Override
	public PyramidTextureData loadToRam(PyramidTileIndex index) 
	throws TileLoadError, MissingTileException
	{
		int z = index.getZ();
		// Raveler uses a separate directory for each group of 1000 slices
		String thousands_dir = "";
		if (z >= 1000) {
			thousands_dir = Integer.toString(z/1000) + "000/";
		}
		String path = String.format(
				"tiles/1024/%d/%d/%d/g/%s%03d.png",
				index.getZoom(),
				index.getY(),
				index.getX(),
				thousands_dir, z);
		URL url;
		try {
			url = new URL(urlStalk, path);
		} catch (MalformedURLException e) {
			throw new TileLoadError(e);
		}
		// log.info("Loading texture from " + url);
		// TODO - download and parse simultaneously to save time
		// AFTER performance optimization is complete
		BufferedImage image;
		try {
			ByteArrayInputStream byteStream = downloadBytes(url);
			image = decodeImage(byteStream);
		} catch (IOException e) {
			throw new TileLoadError(e);
		}
		return convertToGlFormat(image);
	}

	protected static PyramidTextureData convertToGlFormat(BufferedImage image) 
	throws TileLoadError 
	{
		ColorModel colorModel = image.getColorModel();
		// NOT getNumColorComponents(), because we count alpha channel as data.
		int channelCount = colorModel.getNumComponents();
		int bitDepth = colorModel.getPixelSize() / channelCount;
		boolean isSrgb = colorModel.getColorSpace().isCS_sRGB();
		// Determine correct OpenGL texture type, based on bit-depth, number of colors, and srgb
		int internalFormat, pixelFormat;
		boolean isSrgbApplied = false;
		if (channelCount == 1) {
			internalFormat = pixelFormat = GL2.GL_LUMINANCE; // default for single gray channel
			if (bitDepth > 8)
				internalFormat = GL2.GL_LUMINANCE16;
		}
		else if (channelCount == 2) {
			internalFormat = pixelFormat = GL2.GL_LUMINANCE_ALPHA;
			if (bitDepth > 8)
				internalFormat = GL2.GL_LUMINANCE16_ALPHA16;
		}
		else if (channelCount == 3) {
			internalFormat = pixelFormat = GL2.GL_RGB;
			if (bitDepth > 8)
				internalFormat = GL2.GL_RGB16;
			else if (isSrgb) {
				internalFormat = GL2.GL_SRGB;
				isSrgbApplied = true;
			}
		}
		else if (channelCount == 4) {
			internalFormat = pixelFormat = GL2.GL_RGB8;
			if (bitDepth > 8)
				internalFormat = GL2.GL_RGB16;				
			else if (isSrgb) {
				internalFormat = GL2.GL_SRGB_ALPHA;
				isSrgbApplied = true;
			}
		}
		else {
			throw new TileLoadError("Unsupported number of channels");
		}

		PyramidTextureData result = new JoglTextureData(
				AWTTextureIO.newTextureData(
				SliceViewer.glProfile,
				image,
				internalFormat,
				pixelFormat,
				false)); // mipmap
		return result;
	}

}

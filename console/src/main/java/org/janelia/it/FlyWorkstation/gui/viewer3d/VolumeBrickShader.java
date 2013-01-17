/**
 * This class will allow dynamic selection of colors to present in the renderer, by forwarding parameters changed
 * programmatically, onward into the shader-language implementations.
 */
package org.janelia.it.FlyWorkstation.gui.viewer3d;

import javax.media.opengl.GL;
import javax.media.opengl.GL2;
import java.nio.IntBuffer;

public class VolumeBrickShader extends AbstractShader {
    public static final String VERTEX_SHADER = "shaders/VolumeBrickVtx.glsl";
    public static final String FRAGMENT_SHADER = "shaders/VolumeBrickFrg.glsl";

    private static final float[] SHOW_ALL  = new float[] {
        1.0f, 1.0f, 1.0f
    };

    private int previousShader = 0;
    private float[] rgb;

    private boolean volumeMaskApplied = false;

    @Override
    public String getVertexShader() {
        return VERTEX_SHADER;
    }

    @Override
    public String getFragmentShader() {
        return FRAGMENT_SHADER;
    }

    public void load(GL2 gl) {
        IntBuffer buffer = IntBuffer.allocate( 1 );
        gl.glGetIntegerv( GL2.GL_CURRENT_PROGRAM, buffer );
        previousShader = buffer.get();
        int shaderProgram = getShaderProgram();

        gl.glUseProgram( shaderProgram );

        pushMaskUniform( gl, shaderProgram );
        pushFilterUniform( gl, shaderProgram );
        setTextureUniforms( gl );
    }

    public void setColorMask( float[] rgb ) {
        this.rgb = rgb;
    }

    /** Calling this implies that all steps for setting up this special texture have been carried out. */
    public void setVolumeMaskApplied() {
        volumeMaskApplied = true;
    }

    public void unload(GL2 gl) {
        gl.glUseProgram(previousShader);
    }

    private void setTextureUniforms(GL2 gl) {
        int signalTextureLoc = gl.glGetUniformLocation(getShaderProgram(), "signalTexture");
        if ( signalTextureLoc == -1 ) {
            throw new RuntimeException( "Failed to find signal texture location." );
        }
        gl.glUniform1i( signalTextureLoc, 0 );
        //  This did not work.  GL.GL_TEXTURE0 ); //textureIds[ 0 ] );

        if ( volumeMaskApplied ) {
            int maskingTextureLoc = gl.glGetUniformLocation(getShaderProgram(), "maskingTexture");
            if ( maskingTextureLoc == -1 ) {
                throw new RuntimeException( "Failed to find masking texture location." );
            }
            gl.glUniform1i( maskingTextureLoc, 1 );
            // This did not work.  GL.GL_TEXTURE1 ); //textureIds[ 1 ] );
        }
    }

    private void pushMaskUniform( GL2 gl, int shaderProgram ) {
        // Need to push uniform for masking parameter.
        int hasMaskLoc = gl.glGetUniformLocation(shaderProgram, "hasMaskingTexture");
        if ( hasMaskLoc == -1 ) {
            throw new RuntimeException( "Failed to find masking texture flag location." );
        }
        gl.glUniform1i( hasMaskLoc, volumeMaskApplied ? 1 : 0 );

    }

    private void pushFilterUniform(GL2 gl, int shaderProgram) {
        // Need to push uniform for the filtering parameter.
        int colorMaskLoc = gl.glGetUniformLocation(shaderProgram, "colorMask");
        if ( colorMaskLoc == -1 ) {
            throw new RuntimeException( "Failed to find color mask uniform location." );
        }

        float[] localrgb = null;
        if ( rgb == null ) {
            localrgb = SHOW_ALL;
        }
        else {
            localrgb = rgb;
        }

        gl.glUniform4f(
                colorMaskLoc,
                localrgb[0],
                localrgb[1],
                localrgb[2],
                1.0f
        );

    }

}

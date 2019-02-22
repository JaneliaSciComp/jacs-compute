package org.janelia.jacs2.cdi;

import org.janelia.jacs2.cdi.qualifier.IntPropertyValue;
import org.janelia.rendering.CachedRenderedVolumeLoader;
import org.janelia.rendering.RenderedVolumeLoader;
import org.janelia.rendering.RenderedVolumeLoaderImpl;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.Produces;

@ApplicationScoped
public class RenderedVolumeProducer {

    @ApplicationScoped
    @Produces
    public RenderedVolumeLoader createRenderedVolumeLoader(@IntPropertyValue(name = "RenderedVolumesCacheSize", defaultValue = 100) int renderedVolumesCacheSize,
                                                           @IntPropertyValue(name = "RenderedTileImagesCacheSize", defaultValue = 100) int renderedTileImagesCacheSize) {
        return new CachedRenderedVolumeLoader(new RenderedVolumeLoaderImpl(), renderedVolumesCacheSize, renderedTileImagesCacheSize);
    }
}

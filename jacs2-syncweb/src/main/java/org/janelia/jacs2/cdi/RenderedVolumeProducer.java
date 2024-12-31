package org.janelia.jacs2.cdi;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.Produces;

import org.janelia.rendering.RenderedVolumeLoader;
import org.janelia.rendering.RenderedVolumeLoaderImpl;

@ApplicationScoped
public class RenderedVolumeProducer {

    @ApplicationScoped
    @Produces
    public RenderedVolumeLoader createRenderedVolumeLoader() {
        return new RenderedVolumeLoaderImpl();
    }
}

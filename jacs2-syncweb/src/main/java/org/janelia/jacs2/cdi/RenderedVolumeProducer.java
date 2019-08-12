package org.janelia.jacs2.cdi;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.Produces;

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

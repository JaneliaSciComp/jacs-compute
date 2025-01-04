package org.janelia.jacs2.cdi;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Produces;

import org.janelia.rendering.RenderedVolumeLoader;
import org.janelia.rendering.RenderedVolumeLoaderImpl;

@ApplicationScoped
public class RenderedVolumeProducer {

    @Default
    @Produces
    public RenderedVolumeLoader createRenderedVolumeLoader() {
        return new RenderedVolumeLoaderImpl();
    }

}

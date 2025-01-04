package org.janelia.jacs2.cdi;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

import org.janelia.jacs2.job.BackgroundJobs;

@ApplicationScoped
public class WebApplicationProducer {
    @ApplicationScoped
    @Produces
    public BackgroundJobs jobs() {
        return new BackgroundJobs();
    }
}

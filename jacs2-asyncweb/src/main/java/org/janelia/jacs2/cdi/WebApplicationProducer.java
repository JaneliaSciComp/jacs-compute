package org.janelia.jacs2.cdi;

import org.janelia.jacs2.job.BackgroundJobs;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

public class WebApplicationProducer {

    @ApplicationScoped
    @Produces
    public BackgroundJobs jobs() {
        return new BackgroundJobs();
    }

}

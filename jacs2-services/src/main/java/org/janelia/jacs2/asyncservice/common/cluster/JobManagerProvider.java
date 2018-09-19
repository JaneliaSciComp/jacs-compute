package org.janelia.jacs2.asyncservice.common.cluster;

import org.janelia.cluster.JobManager;
import org.janelia.cluster.lsf.LsfSyncApi;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * Attaches a monitoring thread to a JobManager.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@ApplicationScoped
public class JobManagerProvider {

    private final JobManager jobMgr;

    public JobManagerProvider() {
        this.jobMgr = new JobManager(new LsfSyncApi());
    }

    public JobManager get() {
        return jobMgr;
    }
}

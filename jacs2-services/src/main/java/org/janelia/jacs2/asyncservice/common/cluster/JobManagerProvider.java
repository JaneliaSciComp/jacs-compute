package org.janelia.jacs2.asyncservice.common.cluster;

import org.janelia.cluster.JobManager;
import org.janelia.cluster.lsf.LsfSyncApi;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;

/**
 * Attaches a monitoring thread to a JobManager.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Dependent
public class JobManagerProvider {

    private final JobManager jobMgr;

    public JobManagerProvider() {
        this.jobMgr = new JobManager(new LsfSyncApi());
    }

    public JobManager get() {
        return jobMgr;
    }
}

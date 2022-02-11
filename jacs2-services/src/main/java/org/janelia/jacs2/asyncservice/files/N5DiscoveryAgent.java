package org.janelia.jacs2.asyncservice.files;

import org.janelia.model.access.domain.dao.SyncedPathDao;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * A file discovery agent which finds N5 containers and creates corresponding N5Container objects.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("n5DiscoveryAgent")
public class N5DiscoveryAgent implements FileDiscoveryAgent {

    @Inject
    private SyncedPathDao syncedPathDao;

    public void discover() {



    }

}

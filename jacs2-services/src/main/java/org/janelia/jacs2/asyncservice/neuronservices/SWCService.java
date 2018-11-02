package org.janelia.jacs2.asyncservice.neuronservices;

import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;

import javax.inject.Inject;
import java.util.List;

public class SWCService {

    private final TmWorkspaceDao tmWorkspaceDao;

    @Inject
    public SWCService(TmWorkspaceDao tmWorkspaceDao) {
        this.tmWorkspaceDao = tmWorkspaceDao;
    }

    public TmWorkspace importSWVFolder(String swcFolderName, Long sampleId, String workspaceName, String workspaceOwnerKey, List<String> accessibleBy) {
        TmWorkspace tmWorkspace = tmWorkspaceDao.createTmWorkspace(workspaceOwnerKey, createWorkspace(sampleId, workspaceName, accessibleBy));
        // TODO !!!!
        return tmWorkspace;
    }

    private TmWorkspace createWorkspace(Long sampleId, String workspaceName, List<String> accessibleBy) {
        TmWorkspace tmWorkspace = new TmWorkspace(workspaceName, sampleId);
        tmWorkspace.getReaders().addAll(accessibleBy);
        tmWorkspace.getWriters().addAll(accessibleBy);
        return tmWorkspace;
    }
}

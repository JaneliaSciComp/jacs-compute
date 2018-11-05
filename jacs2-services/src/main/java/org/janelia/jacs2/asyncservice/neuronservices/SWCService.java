package org.janelia.jacs2.asyncservice.neuronservices;

import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.domain.dao.TmSampleDao;
import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
import org.janelia.model.rendering.RenderedVolumeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class SWCService {

    private static final Logger LOG = LoggerFactory.getLogger(SWCService.class);

    private final TmSampleDao tmSampleDao;
    private final TmWorkspaceDao tmWorkspaceDao;
    @Inject
    private RenderedVolumeLoader renderedVolumeLoader;
    private final Path defaultSWCLocation;

    @Inject
    public SWCService(TmSampleDao tmSampleDao,
                      TmWorkspaceDao tmWorkspaceDao,
                      @PropertyValue(name = "service.swcImport.DefaultLocation") String defaultSWCLocation) {
        this.tmSampleDao = tmSampleDao;
        this.tmWorkspaceDao = tmWorkspaceDao;
        this.defaultSWCLocation = Paths.get(defaultSWCLocation);
    }

    public TmWorkspace importSWVFolder(String swcFolderName, Long sampleId, String workspaceName, String workspaceOwnerKey, List<String> accessUsers) {
        TmSample tmSample = tmSampleDao.findByIdAndSubjectKey(sampleId, workspaceOwnerKey);
        if (tmSample == null) {
            LOG.error("Sample {} either does not exist or user {} has no access to it", sampleId, workspaceOwnerKey);
            throw new IllegalArgumentException("Sample " + sampleId + " either does not exist or is not accessible");
        }
        TmWorkspace tmWorkspace = tmWorkspaceDao.createTmWorkspace(workspaceOwnerKey, createWorkspace(swcFolderName, sampleId, workspaceName, accessUsers));
        renderedVolumeLoader.loadVolume(Paths.get(tmSample.getFilepath()));
        // TODO !!!!
        return tmWorkspace;
    }

    private TmWorkspace createWorkspace(String swcFolderName, Long sampleId, String workspaceName, List<String> accessUsers) {
        Path swcPath = Paths.get(swcFolderName);
        Path swcBasePath;
        if (swcPath.isAbsolute()) {
            swcBasePath = swcPath;
        } else {
            swcBasePath = defaultSWCLocation.resolve(swcPath);
        }
        TmWorkspace tmWorkspace = new TmWorkspace(workspaceName, sampleId);
        tmWorkspace.setOriginalSWCPath(swcBasePath.toString());
        tmWorkspace.getReaders().addAll(accessUsers);
        tmWorkspace.getWriters().addAll(accessUsers);
        return tmWorkspace;
    }
}

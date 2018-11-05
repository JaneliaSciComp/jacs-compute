package org.janelia.jacs2.dataservice.swc;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.domain.dao.TmSampleDao;
import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
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
    private final RenderedVolumeLoader renderedVolumeLoader;
    private final SWCReader swcReader;
    private final Path defaultSWCLocation;

    @Inject
    public SWCService(TmSampleDao tmSampleDao,
                      TmWorkspaceDao tmWorkspaceDao,
                      RenderedVolumeLoader renderedVolumeLoader,
                      SWCReader swcReader,
                      @PropertyValue(name = "service.swcImport.DefaultLocation") String defaultSWCLocation) {
        this.tmSampleDao = tmSampleDao;
        this.tmWorkspaceDao = tmWorkspaceDao;
        this.renderedVolumeLoader = renderedVolumeLoader;
        this.swcReader = swcReader;
        this.defaultSWCLocation = Paths.get(defaultSWCLocation);
    }

    public TmWorkspace importSWVFolder(String swcFolderName, Long sampleId, String workspaceName, String workspaceOwnerKey, List<String> accessUsers) {
        TmSample tmSample = tmSampleDao.findByIdAndSubjectKey(sampleId, workspaceOwnerKey);
        if (tmSample == null) {
            LOG.error("Sample {} either does not exist or user {} has no access to it", sampleId, workspaceOwnerKey);
            throw new IllegalArgumentException("Sample " + sampleId + " either does not exist or is not accessible");
        }
        TmWorkspace tmWorkspace = tmWorkspaceDao.createTmWorkspace(workspaceOwnerKey, createWorkspace(swcFolderName, sampleId, workspaceName, accessUsers));
//!!!        renderedVolumeLoader.loadVolume(Paths.get(tmSample.getFilepath()));
        // TODO !!!!
        return tmWorkspace;
    }

    private TmWorkspace createWorkspace(String swcFolderName, Long sampleId, String workspaceNameParam, List<String> accessUsers) {
        Path swcPath = Paths.get(swcFolderName);
        Path swcBasePath;
        if (swcPath.isAbsolute()) {
            swcBasePath = swcPath;
        } else {
            swcBasePath = defaultSWCLocation.resolve(swcPath);
        }
        String workspaceName = StringUtils.defaultIfBlank(workspaceNameParam, swcPath.getFileName().toString());
        TmWorkspace tmWorkspace = new TmWorkspace(workspaceName.trim(), sampleId);
        tmWorkspace.setOriginalSWCPath(swcBasePath.toString());
        tmWorkspace.getReaders().addAll(accessUsers);
        tmWorkspace.getWriters().addAll(accessUsers);
        return tmWorkspace;
    }

    private TmNeuronMetadata importSWCFile(Path swcFile, TmWorkspace tmWorkspace) {
        SWCData swcData = swcReader.readSWCFile(swcFile);

        // externalOffset is because Vaa3d cannot handle large coordinates in swc
        // se we added an OFFSET header and recentered on zero when exporting
        double[] externalOffset = swcData.extractOffset();
        String neuronName = swcData.extractName();
        return null; // !!!!!!!!!!
    }

}

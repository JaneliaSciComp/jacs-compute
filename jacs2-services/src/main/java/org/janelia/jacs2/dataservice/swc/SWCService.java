package org.janelia.jacs2.dataservice.swc;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.TmNeuronBufferDao;
import org.janelia.model.access.domain.dao.TmSampleDao;
import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.access.tiledMicroscope.TmModelManipulator;
import org.janelia.model.domain.tiledMicroscope.TmGeoAnnotation;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmProtobufExchanger;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
import org.janelia.model.rendering.RenderedVolume;
import org.janelia.model.rendering.RenderedVolumeLoader;
import org.janelia.model.util.IdSource;
import org.janelia.model.util.MatrixUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.awt.*;
import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SWCService {

    private static final Logger LOG = LoggerFactory.getLogger(SWCService.class);

    private final LegacyDomainDao domainDao;
    private final TmSampleDao tmSampleDao;
    private final TmWorkspaceDao tmWorkspaceDao;
    private final TmNeuronBufferDao tmNeuronBufferDao;
    private final RenderedVolumeLoader renderedVolumeLoader;
    private final SWCReader swcReader;
    private final Path defaultSWCLocation;
    private final IdSource neuronIdGenerator;
    private final TmModelManipulator neuronManipulator;
    private final TmProtobufExchanger protobufExchanger;

    @Inject
    public SWCService(LegacyDomainDao domainDao,
                      TmSampleDao tmSampleDao,
                      TmWorkspaceDao tmWorkspaceDao,
                      TmNeuronBufferDao tmNeuronBufferDao,
                      RenderedVolumeLoader renderedVolumeLoader,
                      SWCReader swcReader,
                      IdSource neuronIdGenerator,
                      TmProtobufExchanger protobufExchanger,
                      @PropertyValue(name = "service.swcImport.DefaultLocation") String defaultSWCLocation) {
        this.domainDao = domainDao;
        this.tmSampleDao = tmSampleDao;
        this.tmWorkspaceDao = tmWorkspaceDao;
        this.tmNeuronBufferDao = tmNeuronBufferDao;
        this.renderedVolumeLoader = renderedVolumeLoader;
        this.swcReader = swcReader;
        this.neuronIdGenerator = neuronIdGenerator;
        this.protobufExchanger = protobufExchanger;
        this.defaultSWCLocation = Paths.get(defaultSWCLocation);
        this.neuronManipulator = new TmModelManipulator(null, neuronIdGenerator);
    }

    public TmWorkspace importSWCFolder(String swcFolderName, Long sampleId, String neuronOwnerKey, String workspaceName, String workspaceOwnerKey, List<String> accessUsers) {
        TmSample tmSample = tmSampleDao.findByIdAndSubjectKey(sampleId, workspaceOwnerKey);
        if (tmSample == null) {
            LOG.error("Sample {} either does not exist or user {} has no access to it", sampleId, workspaceOwnerKey);
            throw new IllegalArgumentException("Sample " + sampleId + " either does not exist or is not accessible");
        }
        TmWorkspace tmWorkspace = tmWorkspaceDao.createTmWorkspace(workspaceOwnerKey, createWorkspace(swcFolderName, sampleId, workspaceName, accessUsers));
        RenderedVolume renderedVolume = renderedVolumeLoader.loadVolume(Paths.get(tmSample.getFilepath()))
                .orElseThrow(() -> new IllegalStateException("Error loading volume metadata for sample " + sampleId));

        VectorOperator externalToInternalConverter = new JamaMatrixVectorOperator(
                MatrixUtilities.buildMicronToVox(renderedVolume.getMicromsPerVoxel(), renderedVolume.getOriginVoxel()));

        FileUtils.lookupFiles(Paths.get(swcFolderName), 3, "glob:**/*.swc")
                .forEach(swcFile -> {
                    TmNeuronMetadata neuronMetadata = importSWCFile(swcFile, neuronOwnerKey, tmWorkspace, externalToInternalConverter);
                    try {
                        tmNeuronBufferDao.createNeuronWorkspacePoints(neuronMetadata.getId(), tmWorkspace.getId(), new ByteArrayInputStream(protobufExchanger.serializeNeuron(neuronMetadata)));
                    } catch (Exception e) {
                        LOG.error("Error creating neuron points while importing {} into {}", swcFile, neuronMetadata, e);
                        throw new IllegalStateException(e);
                    }
                });
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

    private TmNeuronMetadata importSWCFile(Path swcFile, String neuronOwnerKey, TmWorkspace tmWorkspace, VectorOperator externalToInternalConverter) {
        SWCData swcData = swcReader.readSWCFile(swcFile);

        // externalOffset is because Vaa3d cannot handle large coordinates in swc
        // se we added an OFFSET header and recentered on zero when exporting
        double[] externalOffset = swcData.extractOffset();
        String neuronName = swcData.extractName();
        TmNeuronMetadata neuronMetadata = new TmNeuronMetadata(tmWorkspace, neuronName);
        neuronMetadata.setReaders(tmWorkspace.getReaders());
        neuronMetadata.setWriters(tmWorkspace.getWriters());
        neuronMetadata.setId(neuronIdGenerator.next());

        Map<Integer, Integer> nodeParentLinkage = new HashMap<>();
        Map<Integer, TmGeoAnnotation> annotations = new HashMap<>();

        Date now = new Date();
        for (SWCNode node : swcData.getNodeList()) {
            // Internal points, as seen in annotations, are same as external
            // points in SWC: represented as voxels. --LLF
            double[] internalPoint = externalToInternalConverter.apply(new double[] {
                    node.getX() + externalOffset[0],
                    node.getY() + externalOffset[1],
                    node.getZ() + externalOffset[2]
            });
            TmGeoAnnotation unserializedAnnotation = new TmGeoAnnotation(
                    new Long(node.getIndex()), null, neuronMetadata.getId(),
                    internalPoint[0], internalPoint[1], internalPoint[2], node.getRadius(),
                    now, now
            );

            annotations.put(node.getIndex(), unserializedAnnotation);
            nodeParentLinkage.put(node.getIndex(), node.getParentIndex());

        }
        neuronManipulator.addLinkedGeometricAnnotationsInMemory(nodeParentLinkage, annotations, neuronMetadata);

        // Set neuron color
        float[] colorArr = swcData.extractColors();
        if (colorArr != null) {
            Color color = new Color(colorArr[0], colorArr[1], colorArr[2]);
            neuronMetadata.setColor(color);
        }

        try {
            return domainDao.createWithPrepopulatedId(neuronOwnerKey, neuronMetadata);
        } catch (Exception e) {
            LOG.error("Error while trying to persist {}", neuronMetadata, e);
            throw new IllegalStateException(e);
        }
    }

}

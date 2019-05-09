package org.janelia.jacs2.dataservice.swc;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.rendering.RenderedVolumeLocationFactory;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.IdSource;
import org.janelia.model.access.domain.dao.TmNeuronBufferDao;
import org.janelia.model.access.domain.dao.TmSampleDao;
import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.access.tiledMicroscope.TmModelManipulator;
import org.janelia.model.domain.tiledMicroscope.TmGeoAnnotation;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmProtobufExchanger;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
import org.janelia.model.util.MatrixUtilities;
import org.janelia.rendering.RenderedVolume;
import org.janelia.rendering.RenderedVolumeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.awt.*;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class SWCService {

    private static final Logger LOG = LoggerFactory.getLogger(SWCService.class);

    private final StorageService storageService;
    private final LegacyDomainDao domainDao;
    private final TmSampleDao tmSampleDao;
    private final TmWorkspaceDao tmWorkspaceDao;
    private final TmNeuronBufferDao tmNeuronBufferDao;
    private final RenderedVolumeLocationFactory renderedVolumeLocationFactory;
    private final RenderedVolumeLoader renderedVolumeLoader;
    private final SWCReader swcReader;
    private final Path defaultSWCLocation;
    private final IdSource neuronIdGenerator;
    private final TmModelManipulator neuronManipulator;
    private final TmProtobufExchanger protobufExchanger;

    @Inject
    public SWCService(StorageService storageService,
                      LegacyDomainDao domainDao,
                      TmSampleDao tmSampleDao,
                      TmWorkspaceDao tmWorkspaceDao,
                      TmNeuronBufferDao tmNeuronBufferDao,
                      RenderedVolumeLocationFactory renderedVolumeLocationFactory,
                      RenderedVolumeLoader renderedVolumeLoader,
                      SWCReader swcReader,
                      IdSource neuronIdGenerator,
                      TmProtobufExchanger protobufExchanger,
                      @PropertyValue(name = "service.swcImport.DefaultLocation") String defaultSWCLocation) {
        this.storageService = storageService;
        this.domainDao = domainDao;
        this.tmSampleDao = tmSampleDao;
        this.tmWorkspaceDao = tmWorkspaceDao;
        this.tmNeuronBufferDao = tmNeuronBufferDao;
        this.renderedVolumeLocationFactory = renderedVolumeLocationFactory;
        this.renderedVolumeLoader = renderedVolumeLoader;
        this.swcReader = swcReader;
        this.neuronIdGenerator = neuronIdGenerator;
        this.protobufExchanger = protobufExchanger;
        this.defaultSWCLocation = StringUtils.isNotBlank(defaultSWCLocation)
            ? Paths.get(defaultSWCLocation)
            : Paths.get("");
        this.neuronManipulator = new TmModelManipulator(null, neuronIdGenerator);
    }

    public TmWorkspace importSWCFolder(String swcFolderName, Long sampleId,
                                       String workspaceName, String workspaceOwnerKey,
                                       String neuronOwnerKey,
                                       List<String> accessUsers) {
        TmSample tmSample = tmSampleDao.findEntityByIdAccessibleBySubjectKey(sampleId, workspaceOwnerKey);
        if (tmSample == null) {
            LOG.error("Sample {} either does not exist or user {} has no access to it", sampleId, workspaceOwnerKey);
            throw new IllegalArgumentException("Sample " + sampleId + " either does not exist or is not accessible");
        }
        TmWorkspace tmWorkspace = tmWorkspaceDao.createTmWorkspace(workspaceOwnerKey, createWorkspace(swcFolderName, sampleId, workspaceName));
        accessUsers.forEach(accessUserKey -> {
            try {
                domainDao.setPermissions(workspaceOwnerKey, TmWorkspace.class.getName(), tmWorkspace.getId(), accessUserKey, true, true, true);
            } catch (Exception e) {
                LOG.error("Error giving permission on {} to {}", tmWorkspace, accessUserKey, e);
            }
            try {
                domainDao.setPermissions(tmSample.getOwnerKey(), TmSample.class.getName(), tmSample.getId(), accessUserKey, true, true, true);
            } catch (Exception e) {
                LOG.error("Error giving permission on {} to {}", tmWorkspace, accessUserKey, e);
            }
        });
        RenderedVolume renderedVolume = renderedVolumeLoader.loadVolume(renderedVolumeLocationFactory.getVolumeLocation(tmSample.getFilepath(), workspaceOwnerKey, null))
                .orElseThrow(() -> new IllegalStateException("Error loading volume metadata for sample " + sampleId));

        VectorOperator externalToInternalConverter = new JamaMatrixVectorOperator(
                MatrixUtilities.buildMicronToVox(renderedVolume.getMicromsPerVoxel(), renderedVolume.getOriginVoxel()));

        LOG.info("Lookup SWC folder {}", swcFolderName);
        storageService.lookupStorageVolumes(null, null, swcFolderName, null, null)
                .map(vsInfo -> {
                    LOG.info("Found {} for SWC folder {}", vsInfo, swcFolderName);
                    String swcPath;
                    if (swcFolderName.startsWith(vsInfo.getStorageVirtualPath())) {
                        swcPath = Paths.get(vsInfo.getStorageVirtualPath()).relativize(Paths.get(swcFolderName)).toString();
                    } else {
                        swcPath = Paths.get(vsInfo.getBaseStorageRootDir()).relativize(Paths.get(swcFolderName)).toString();
                    }
                    LOG.info("List swc entries on {} : {}", vsInfo, swcPath);
                    return storageService.listStorageContent(vsInfo.getStorageURL(), swcPath, null, null, 3).stream();
                })
                .orElseGet(() -> Stream.of())
                .filter(storageEntryInfo -> storageEntryInfo.getEntryRelativePath().endsWith(".swc"))
                .forEach(swcEntry ->{
                    LOG.info("Read swcEntry {} from {}", swcEntry, swcEntry.getEntryURL());
                    InputStream swcStream = storageService.getStorageContent(swcEntry.getEntryURL(), null, null);
                    TmNeuronMetadata neuronMetadata = importSWCFile(swcEntry.getEntryRelativePath(), swcStream, neuronOwnerKey, tmWorkspace, externalToInternalConverter);
                    try {
                        LOG.info("Create neuron points for swcEntry {} in workspace {} for sample {}", swcEntry, tmWorkspace, tmSample);
                        tmNeuronBufferDao.createNeuronWorkspacePoints(neuronMetadata.getId(), tmWorkspace.getId(), new ByteArrayInputStream(protobufExchanger.serializeNeuron(neuronMetadata)));
                    } catch (Exception e) {
                        LOG.error("Error creating neuron points while importing {} into {}", swcEntry, neuronMetadata, e);
                        throw new IllegalStateException(e);
                    }
                });
        return tmWorkspace;
    }

    private TmWorkspace createWorkspace(String swcFolderName, Long sampleId, String workspaceNameParam) {
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
        return tmWorkspace;
    }

    private TmNeuronMetadata importSWCFile(String swcEntryName, InputStream swcStream, String neuronOwnerKey, TmWorkspace tmWorkspace, VectorOperator externalToInternalConverter) {
        SWCData swcData = swcReader.readSWCStream(swcEntryName, swcStream);

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

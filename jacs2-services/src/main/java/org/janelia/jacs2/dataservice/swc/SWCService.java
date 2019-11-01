package org.janelia.jacs2.dataservice.swc;

import java.awt.Color;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PushbackInputStream;
import java.io.UncheckedIOException;
import java.net.URLDecoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import com.google.common.io.ByteStreams;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.data.NamedData;
import org.janelia.jacs2.dataservice.storage.DataStorageLocationFactory;
import org.janelia.jacs2.dataservice.storage.StorageEntryInfo;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.IdSource;
import org.janelia.model.access.domain.dao.TmNeuronMetadataDao;
import org.janelia.model.access.domain.dao.TmSampleDao;
import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.domain.tiledMicroscope.TmGeoAnnotation;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
import org.janelia.model.util.MatrixUtilities;
import org.janelia.model.util.TmNeuronUtils;
import org.janelia.rendering.RenderedVolumeLoader;
import org.janelia.rendering.RenderedVolumeLocation;
import org.janelia.rendering.RenderedVolumeMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ScalaLongSignature;

public class SWCService {

    private static final Logger LOG = LoggerFactory.getLogger(SWCService.class);

    private final StorageService storageService;
    private final LegacyDomainDao domainDao;
    private final TmSampleDao tmSampleDao;
    private final TmWorkspaceDao tmWorkspaceDao;
    private final TmNeuronMetadataDao tmNeuronMetadataDao;
    private final DataStorageLocationFactory dataStorageLocationFactory;
    private final RenderedVolumeLoader renderedVolumeLoader;
    private final SWCReader swcReader;
    private final Path defaultSWCLocation;
    private final IdSource neuronIdGenerator;

    @Inject
    public SWCService(StorageService storageService,
                      LegacyDomainDao domainDao,
                      TmSampleDao tmSampleDao,
                      TmWorkspaceDao tmWorkspaceDao,
                      TmNeuronMetadataDao tmNeuronMetadataDao,
                      DataStorageLocationFactory dataStorageLocationFactory,
                      RenderedVolumeLoader renderedVolumeLoader,
                      SWCReader swcReader,
                      IdSource neuronIdGenerator,
                      @PropertyValue(name = "service.swcImport.DefaultLocation") String defaultSWCLocation) {
        this.storageService = storageService;
        this.domainDao = domainDao;
        this.tmSampleDao = tmSampleDao;
        this.tmWorkspaceDao = tmWorkspaceDao;
        this.tmNeuronMetadataDao = tmNeuronMetadataDao;
        this.dataStorageLocationFactory = dataStorageLocationFactory;
        this.renderedVolumeLoader = renderedVolumeLoader;
        this.swcReader = swcReader;
        this.neuronIdGenerator = neuronIdGenerator;
        this.defaultSWCLocation = StringUtils.isNotBlank(defaultSWCLocation)
            ? Paths.get(defaultSWCLocation)
            : Paths.get("");
    }

    public TmWorkspace importSWCFolder(String swcFolderName, Long sampleId,
                                       String workspaceName, String workspaceOwnerKey,
                                       String neuronOwnerKey,
                                       List<String> accessUsers,
                                       long firstEntryOffset,
                                       boolean orderSWCs) {
        TmSample tmSample = tmSampleDao.findEntityByIdReadableBySubjectKey(sampleId, workspaceOwnerKey);
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
        String sampleFilepath = tmSample.getLargeVolumeOctreeFilepath();
        RenderedVolumeLocation volumeLocation = dataStorageLocationFactory.asRenderedVolumeLocation(dataStorageLocationFactory.getDataLocationWithLocalCheck(sampleFilepath, workspaceOwnerKey, null));
        RenderedVolumeMetadata renderedVolumeMetadata = renderedVolumeLoader.loadVolume(volumeLocation)
                .orElseThrow(() -> {
                    LOG.error("Could not load volume metadata for sample {} from {}", sampleId, volumeLocation.getBaseStorageLocationURI());
                    return new IllegalStateException("Error loading volume metadata for sample " + sampleId + " from " + volumeLocation.getBaseStorageLocationURI());
                });

        VectorOperator externalToInternalConverter = new JamaMatrixVectorOperator(
                MatrixUtilities.buildMicronToVox(renderedVolumeMetadata.getMicromsPerVoxel(), renderedVolumeMetadata.getOriginVoxel()));

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
                    String swcStorageFolderURL = storageService.getEntryURI(vsInfo.getVolumeStorageURI(), swcPath);
                    LOG.info("Retrieve swc content from {} : {}", vsInfo, swcPath);

                    Spliterator<NamedData<InputStream>> storageContentSupplier = new Spliterator<NamedData<InputStream>>() {
                        long offset = firstEntryOffset;
                        int defaultLength = 100000;
                        TarArchiveInputStream archiveInputStream = null;
                        TarArchiveEntry currentEntry = null;

                        @Override
                        public boolean tryAdvance(Consumer<? super NamedData<InputStream>> action) {
                            try {
                                for (; ;) {
                                    if (archiveInputStream == null) {
                                        LOG.info("Retrieve entries ({} - {}) from {}", offset, offset + defaultLength, swcStorageFolderURL);
                                        InputStream swcDataStream = storageService.getStorageFolderContent(swcStorageFolderURL, offset, defaultLength, orderSWCs, null, null);
                                        if (swcDataStream == null) {
                                            return false;
                                        }
                                        PipedOutputStream pipedOutputStream = new PipedOutputStream();
                                        PipedInputStream pipedInputStream = new PipedInputStream(pipedOutputStream);
                                        archiveInputStream = new TarArchiveInputStream(pipedInputStream);

                                        Thread thread1 = new Thread(new Runnable() {
                                            long start = offset;
                                            long end = offset + defaultLength;
                                            @Override
                                            public void run() {
                                                try {
                                                    ByteStreams.copy(swcDataStream, pipedOutputStream);
                                                } catch (IOException e) {
                                                } finally {
                                                    try {
                                                        swcDataStream.close();
                                                        pipedOutputStream.close();
                                                    } catch (IOException ignore) {
                                                    }
                                                }
                                                LOG.info("Done entries ({} - {}) from {}", start, end, swcStorageFolderURL);
                                            }
                                        });
                                        thread1.start();

                                        currentEntry = archiveInputStream.getNextTarEntry();
                                        if (currentEntry == null) {
                                            archiveInputStream.close();
                                            return false;
                                        }
                                        offset = offset + defaultLength; // prepare the offset for the next set of records
                                    }

                                    // consume until a non folder entry is found
                                    for (; currentEntry != null && currentEntry.isDirectory(); currentEntry = archiveInputStream.getNextTarEntry());

                                    if (currentEntry == null) {
                                        archiveInputStream.close();
                                        archiveInputStream = null;
                                        continue; // try the next set
                                    }

                                    NamedData<InputStream> entryData = new NamedData<>(currentEntry.getName(), ByteStreams.limit(archiveInputStream, currentEntry.getSize()));
                                    // advance the entry
                                    currentEntry = archiveInputStream.getNextTarEntry();
                                    if (currentEntry == null) {
                                        // if this was the last entry from the archive stream close the archive stream
                                        archiveInputStream.close();
                                        archiveInputStream = null;
                                    }
                                    action.accept(entryData);
                                    return true; // even if this archive stream is done, there might be others
                                }
                            } catch (IOException e) {
                                LOG.error("Error reading or reading from swc archive from {} ({}) offset: {}, length: {}", swcStorageFolderURL, swcFolderName, offset, defaultLength, defaultLength, e);
                                throw new UncheckedIOException(e);
                            }
                        }

                        @Override
                        public Spliterator<NamedData<InputStream>> trySplit() {
                            return null;
                        }

                        @Override
                        public long estimateSize() {
                            return Long.MAX_VALUE;
                        }

                        @Override
                        public int characteristics() {
                            return ORDERED;
                        }
                    };
                    return StreamSupport.stream(storageContentSupplier, true);
                })
                .orElseGet(() -> Stream.of())
                .forEach(swcEntry ->{
                    LOG.debug("Read swcEntry {} from {}", swcEntry.getName(), swcFolderName);
                    InputStream swcStream = swcEntry.getData();
                    TmNeuronMetadata neuronMetadata = importSWCFile(swcEntry.getName(), swcStream, neuronOwnerKey, tmWorkspace, externalToInternalConverter);
                    try {
                        LOG.debug("Persist neuron {} in Workspace {}", neuronMetadata.getName(), tmWorkspace.getName());
                        tmNeuronMetadataDao.createTmNeuronInWorkspace(neuronOwnerKey, neuronMetadata, tmWorkspace);
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
        TmNeuronUtils.addLinkedGeometricAnnotationsInMemory(nodeParentLinkage, annotations, neuronMetadata, () -> neuronIdGenerator.next());

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

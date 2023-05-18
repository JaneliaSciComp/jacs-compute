package org.janelia.jacs2.dataservice.swc;

import com.google.common.io.ByteStreams;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ConcurrentStack;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.data.NamedData;
import org.janelia.jacs2.dataservice.storage.DataStorageLocationFactory;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.jacsstorage.clients.api.JadeStorageService;
import org.janelia.jacsstorage.clients.api.StorageLocation;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.IdGenerator;
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

import javax.inject.Inject;
import java.awt.*;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SWCService {

    private static final Logger LOG = LoggerFactory.getLogger(SWCService.class);

    private static class ArchiveInputStreamPosition {
        private final String streamName;
        private ArchiveInputStream archiveInputStream;
        private final long archiveInputStreamOffset;
        private volatile long archiveEntriesCount;

        ArchiveInputStreamPosition(String streamName, ArchiveInputStream archiveInputStream, long archiveInputStreamOffset) {
            this.streamName = streamName;
            this.archiveInputStream = archiveInputStream;
            this.archiveInputStreamOffset = archiveInputStreamOffset;
            archiveEntriesCount = 0L;
        }

        private synchronized ArchiveEntry nextEntry() {
            try {
                ArchiveEntry nextEntry = this.archiveInputStream.getNextEntry();
                if (nextEntry != null) archiveEntriesCount++;
                return nextEntry;
            } catch (IOException e) {
                LOG.error("Error reading next archive entry from {}", streamName, e);
                return null;
            }
        }

        synchronized ArchiveEntry goToNextFileEntry() {
            ArchiveEntry currentEntry = nextEntry();
            for (;
                 currentEntry != null && currentEntry.isDirectory();
                 currentEntry = nextEntry()) {
                // skip directories
            }
            return currentEntry;
        }

        synchronized long getNextOffset() {
            return archiveInputStreamOffset + archiveEntriesCount;
        }

        InputStream getCurrentEntryStream(ArchiveEntry archiveEntry) {
            ByteArrayOutputStream entryStream = new ByteArrayOutputStream();
            try {
                ByteStreams.copy(ByteStreams.limit(archiveInputStream, archiveEntry.getSize()), entryStream);
            } catch (IOException e) {
                LOG.error("Error reading entry {} ({}bytes) from {}",
                        archiveEntry.getName(), archiveEntry.getSize(), streamName);
                return null;
            }
            return new ByteArrayInputStream(entryStream.toByteArray());
        }

        void close() {
            try {
                LOG.debug("Close archive input stream {}:{}", streamName, archiveInputStreamOffset);
                archiveInputStream.close();
            } catch (IOException e) {
                LOG.error("Error closing archive input stream {}:{}", streamName, archiveInputStreamOffset, e);
            } finally {
                archiveInputStream = null;
            }
        }
    }

    private final StorageService storageService;
    private final LegacyDomainDao domainDao;
    private final TmSampleDao tmSampleDao;
    private final TmWorkspaceDao tmWorkspaceDao;
    private final TmNeuronMetadataDao tmNeuronMetadataDao;
    private final DataStorageLocationFactory dataStorageLocationFactory;
    private final RenderedVolumeLoader renderedVolumeLoader;
    private final SWCReader swcReader;
    private final Path defaultSWCLocation;
    private final IdGenerator<Long> neuronIdGenerator;
    private final ExecutorService executorService;
    private final String masterStorageServiceURL;
    private final String storageServiceApiKey;

    @Inject
    public SWCService(StorageService storageService,
                      LegacyDomainDao domainDao,
                      @AsyncIndex TmSampleDao tmSampleDao,
                      @AsyncIndex TmWorkspaceDao tmWorkspaceDao,
                      TmNeuronMetadataDao tmNeuronMetadataDao,
                      DataStorageLocationFactory dataStorageLocationFactory,
                      RenderedVolumeLoader renderedVolumeLoader,
                      SWCReader swcReader,
                      IdGenerator<Long> neuronIdGenerator,
                      @JacsDefault ExecutorService executorService,
                      @PropertyValue(name = "service.swcImport.DefaultLocation") String defaultSWCLocation,
                      @PropertyValue(name = "StorageService.URL") String masterStorageServiceURL,
                      @PropertyValue(name = "StorageService.ApiKey") String storageServiceApiKey) {
        this.storageService = storageService;
        this.masterStorageServiceURL = masterStorageServiceURL;
        this.storageServiceApiKey = storageServiceApiKey;
        this.domainDao = domainDao;
        this.tmSampleDao = tmSampleDao;
        this.tmWorkspaceDao = tmWorkspaceDao;
        this.tmNeuronMetadataDao = tmNeuronMetadataDao;
        this.dataStorageLocationFactory = dataStorageLocationFactory;
        this.renderedVolumeLoader = renderedVolumeLoader;
        this.swcReader = swcReader;
        this.neuronIdGenerator = neuronIdGenerator;
        this.executorService = executorService;
        this.defaultSWCLocation = StringUtils.isNotBlank(defaultSWCLocation)
            ? Paths.get(defaultSWCLocation)
            : Paths.get("");
    }

    public TmWorkspace importSWCFolder(String swcFolderName, Long sampleId,
                                       String workspaceName, String workspaceOwnerKey,
                                       String neuronOwnerKey,
                                       List<String> accessUsers,
                                       long firstEntryOffset,
                                       long maxSize,
                                       int batchSize,
                                       int depth,
                                       boolean orderSWCs) {
        LOG.info("Import SWC folder {} for sample {} into workspace {} for user {} - neuron owner is {}", swcFolderName, sampleId, workspaceName, workspaceOwnerKey, neuronOwnerKey);
        TmSample tmSample = tmSampleDao.findEntityByIdReadableBySubjectKey(sampleId, workspaceOwnerKey);
        if (tmSample == null) {
            LOG.error("Sample {} either does not exist or user {} has no access to it", sampleId, workspaceOwnerKey);
            throw new IllegalArgumentException("Sample " + sampleId + " either does not exist or is not accessible");
        }
        TmWorkspace tmWorkspace = tmWorkspaceDao.createTmWorkspace(workspaceOwnerKey, createWorkspace(swcFolderName, sampleId, workspaceName));
        LOG.info("Created workspace {} for SWC folder {} for sample {} into workspace {} for user {} - neuron owner is {}", tmWorkspace, swcFolderName, sampleId, workspaceName, workspaceOwnerKey, neuronOwnerKey);
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

        return importSWCFolder(swcFolderName, tmSample, tmWorkspace, neuronOwnerKey, firstEntryOffset, maxSize, batchSize, depth, orderSWCs);
    }

    public TmWorkspace importSWCFolder(String swcFolderName, TmSample tmSample, TmWorkspace tmWorkspace,
                                       String neuronOwnerKey,
                                       long firstEntry,
                                       long maxSize,
                                       int batchSize,
                                       int depth,
                                       boolean orderSWCs) {

        VectorOperator externalToInternalConverter = getExternalToInternalConverter(tmSample);

        LOG.info("Lookup SWC folder {}", swcFolderName);
        storageService.findStorageVolumes(swcFolderName, null, null)
                .stream().findFirst()
                .map(vsInfo -> {
                    LOG.info("Found {} for SWC folder {}", vsInfo, swcFolderName);
                    String swcPath;
                    if (swcFolderName.startsWith(StringUtils.appendIfMissing(vsInfo.getStorageVirtualPath(), "/"))) {
                        swcPath = Paths.get(vsInfo.getStorageVirtualPath()).relativize(Paths.get(swcFolderName)).toString();
                    } else if (swcFolderName.startsWith(StringUtils.appendIfMissing(vsInfo.getBaseStorageRootDir(), "/"))) {
                        swcPath = Paths.get(vsInfo.getBaseStorageRootDir()).relativize(Paths.get(swcFolderName)).toString();
                    } else {
                        // the only other option is that the dataPath is actually the root volume path
                        // this may actually be an anomaly
                        swcPath = "";
                    }
                    String swcStorageFolderURL = storageService.getEntryURI(vsInfo.getVolumeStorageURI(), swcPath);
                    LOG.info("Retrieve swc content from {} : {}", vsInfo, swcPath);

                    ConcurrentStack<ArchiveInputStreamPosition> archiveInputStreamStack =
                            initializeStreamStack(swcStorageFolderURL,
                                    firstEntry,
                                    batchSize,
                                    depth,
                                    orderSWCs);
                    Spliterator<NamedData<InputStream>> storageContentSupplier = new Spliterator<NamedData<InputStream>>() {

                        AtomicLong totalEntriesCount = new AtomicLong(0);

                        @Override
                        public boolean tryAdvance(Consumer<? super NamedData<InputStream>> action) {
                            for (; ;) {
                                ArchiveInputStreamPosition currentInputStream = archiveInputStreamStack.top();
                                if (currentInputStream == null) {
                                    // if the stack is empty we are done
                                    LOG.info("Imported {} from {}", totalEntriesCount.get(), swcStorageFolderURL);
                                    return false;
                                }
                                ArchiveEntry currentEntry = currentInputStream.goToNextFileEntry();
                                // just in case some folders got through - ignore them
                                if (currentEntry == null) {
                                    // nothing left in the current stream -> close it
                                    LOG.info("Finished processing {} entries from {}:{}", currentInputStream.archiveEntriesCount, currentInputStream.streamName, currentInputStream.archiveInputStreamOffset);
                                    currentInputStream.close();
                                    // and try to get the next batch
                                    archiveInputStreamStack.pop();
                                    long offset = currentInputStream.getNextOffset();
                                    LOG.info("Fetch next batch from {}:{}", swcStorageFolderURL, offset);
                                    ArchiveInputStreamPosition nextStream = newStream(
                                            swcStorageFolderURL,
                                            openSWCDataStream(swcStorageFolderURL,
                                                    offset,
                                                    batchSize,
                                                    depth,
                                                    orderSWCs),
                                            offset
                                    );
                                    if (nextStream != null) {
                                        // successfully got the next batch
                                        archiveInputStreamStack.push(nextStream);
                                    }
                                    continue;
                                } else {
                                    LOG.debug("Process {} from {}:{}", currentEntry.getName(), swcStorageFolderURL, currentInputStream.archiveInputStreamOffset);
                                    InputStream entryStream = currentInputStream.getCurrentEntryStream(currentEntry);
                                    ArchiveInputStreamPosition archiveEntryStream = newStream(
                                            swcStorageFolderURL + ":" + currentEntry.getName(),
                                            entryStream,
                                            0L);
                                    if (archiveEntryStream != null) {
                                        // the current entry is a zip or tar
                                        archiveInputStreamStack.push(archiveEntryStream);
                                        continue;
                                    } else {
                                        // this is a regular entry => process it
                                        NamedData<InputStream> entryData = new NamedData<>(currentEntry.getName(), entryStream);
                                        action.accept(entryData);
                                        long entriesProcessed = totalEntriesCount.incrementAndGet();
                                        if (maxSize > 0 && entriesProcessed < maxSize) {
                                            return true;
                                        } else {
                                            LOG.info("Finished importing {} from {}", entriesProcessed, currentInputStream.streamName);
                                            // close all stacks
                                            for (ArchiveInputStreamPosition as = archiveInputStreamStack.pop(); as != null; as = archiveInputStreamStack.pop()) {
                                                as.close();
                                            }
                                            return false; // done
                                        }
                                    }
                                }
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
                .orElseGet(Stream::of)
                .forEach(swcEntry ->{
                    LOG.debug("Read swcEntry {} from {}", swcEntry.getName(), swcFolderName);
                    InputStream swcStream = swcEntry.getData();
                    TmNeuronMetadata neuronMetadata = importSWCFile(swcEntry.getName(), swcStream, null,
                            neuronOwnerKey, tmWorkspace, externalToInternalConverter);
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

    /**
     * Reads and returns the rendered volume metadata from the given sample and return the external-to-internal
     * conversion matrix.
     */
    public VectorOperator getExternalToInternalConverter(TmSample tmSample) {

        String sampleFilepath = tmSample.getLargeVolumeOctreeFilepath();
        RenderedVolumeLocation volumeLocation = dataStorageLocationFactory.asRenderedVolumeLocation(dataStorageLocationFactory.getDataLocationWithLocalCheck(sampleFilepath, tmSample.getOwnerKey(), null));
        RenderedVolumeMetadata renderedVolumeMetadata = renderedVolumeLoader.loadVolume(volumeLocation)
                .orElseThrow(() -> {
                    LOG.error("Could not load volume metadata for sample {} from {}", tmSample.getId(), volumeLocation.getBaseStorageLocationURI());
                    return new IllegalStateException("Error loading volume metadata for sample " + tmSample.getId() + " from " + volumeLocation.getBaseStorageLocationURI());
                });

        return new JamaMatrixVectorOperator(
                MatrixUtilities.buildMicronToVox(renderedVolumeMetadata.getMicromsPerVoxel(), renderedVolumeMetadata.getOriginVoxel()));
    }

    /**
     * Import the given SWC file into the given workspace.
     * @param swcFilepath
     * @param tmWorkspace
     * @param neuronName Name for the TmNeuronMetadata object. If null, this will attempt to extract the name from the SWC file.
     * @param neuronOwnerKey
     * @param externalToInternalConverter
     */
    public TmNeuronMetadata importSWC(String swcFilepath, TmWorkspace tmWorkspace,
                                 String neuronName, String neuronOwnerKey, VectorOperator externalToInternalConverter) {

        Path swcPath = Paths.get(swcFilepath);
        String swcFilename = swcPath.getFileName().toString();

        JadeStorageService jadeStorage = new JadeStorageService(masterStorageServiceURL, storageServiceApiKey, null, null);

        StorageLocation storageLocation = jadeStorage.getStorageLocationByPath(swcFilepath);
        if (storageLocation == null) {
            throw new IllegalStateException("Filepath does not exist in Jade: "+swcFilepath);
        }

        try (InputStream swcStream = jadeStorage.getContent(storageLocation, swcFilepath)) {
            TmNeuronMetadata neuronMetadata = importSWCFile(swcFilename, swcStream, neuronName,
                    neuronOwnerKey, tmWorkspace, externalToInternalConverter);
            LOG.debug("Imported neuron {} from {} into {}", neuronMetadata.getName(), swcFilename, tmWorkspace);
            return tmNeuronMetadataDao.createTmNeuronInWorkspace(neuronOwnerKey, neuronMetadata, tmWorkspace);
        }
        catch (Exception e) {
            LOG.error("Error creating neuron while importing {} into {}", swcFilename, tmWorkspace, e);
            throw new IllegalStateException(e);
        }
    }

    private ConcurrentStack<ArchiveInputStreamPosition> initializeStreamStack(String storageURL,
                                                                              long offset,
                                                                              long batchSize,
                                                                              int depth,
                                                                              boolean orderFlag) {
        ConcurrentStack<ArchiveInputStreamPosition> archiveInputStreamStack = new ConcurrentStack<>();
        ArchiveInputStreamPosition archiveInputStream = newStream(
                storageURL,
                openSWCDataStream(
                        storageURL,
                        offset,
                        batchSize,
                        depth,
                        orderFlag),
                offset);
        if (archiveInputStream != null) {
            archiveInputStreamStack.push(archiveInputStream);
        }
        return archiveInputStreamStack;
    }

    private synchronized ArchiveInputStreamPosition newStream(String streamName, InputStream inputStream, long offset) {
        return isArchiveStream(inputStream)
                .map(isArchive -> {
                    if (isArchive) {
                        return new ArchiveInputStreamPosition(streamName, asArchiveInputStream(inputStream), offset);
                    } else {
                        return null;
                    }
                })
                .orElse(null);
    }

    private InputStream openSWCDataStream(String swcStorageFolderURL, long offset, long length, int depth, boolean orderSWCs) {
        LOG.info("Retrieve {} entries from {}:{}", length > 0 ? "all" : length, swcStorageFolderURL, offset);
        InputStream swcDataStream = storageService.getStorageFolderContent(swcStorageFolderURL, offset, length, depth, orderSWCs,
                "\\.(swc|zip|tar|tgz|tar.gz)$",
                null,
                null);
        if (swcDataStream == null) {
            return null;
        }
        try {
            PipedOutputStream pipedOutputStream = new PipedOutputStream();
            PipedInputStream pipedInputStream = new PipedInputStream(pipedOutputStream);

            executorService.submit(() -> {
                try {
                    long nbytes = ByteStreams.copy(swcDataStream, pipedOutputStream);
                    pipedOutputStream.flush();
                    LOG.info("Done retrieving entries ({} - {}) from {} ({} bytes)", offset, offset + length, swcStorageFolderURL, nbytes);
                } catch (IOException e) {
                    LOG.error("Error copying {} bytes from {} starting at {}", length, swcStorageFolderURL, offset, e);
                } finally {
                    try {
                        swcDataStream.close();
                    } catch (IOException ignore) {
                    }
                    try {
                        pipedOutputStream.close();
                    } catch (IOException ignore) {
                    }
                }
            });
            return new BufferedInputStream(pipedInputStream);
        } catch (IOException e) {
            LOG.error("Error retrieving entries ({} - {}) from {}", offset, offset + length, swcStorageFolderURL, e);
            throw new UncheckedIOException(e);
        }
    }

    private Optional<Boolean> isArchiveStream(InputStream inputStream) {
        if (inputStream == null) {
            return Optional.empty();
        } else {
            try {
                ArchiveStreamFactory.detect(inputStream);
                return Optional.of(true);
            } catch (ArchiveException e) {
                return Optional.of(false);
            }
        }
    }

    private ArchiveInputStream asArchiveInputStream(InputStream inputStream) {
        try {
            return new ArchiveStreamFactory()
                    .createArchiveInputStream(inputStream);
        } catch (ArchiveException e) {
            // this should not happen because the stream should be checked before
            throw new IllegalArgumentException(e);
        }
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

    private TmNeuronMetadata importSWCFile(String swcEntryName, InputStream swcStream, String neuronName,
                                           String neuronOwnerKey, TmWorkspace tmWorkspace,
                                           VectorOperator externalToInternalConverter) {

        SWCData swcData = swcReader.readSWCStream(swcEntryName, swcStream);

        // externalOffset is because Vaa3d cannot handle large coordinates in swc
        // se we added an OFFSET header and recentered on zero when exporting
        double[] externalOffset = swcData.extractOffset();
        String name = StringUtils.isBlank(neuronName) ? swcData.extractName() : neuronName;
        TmNeuronMetadata neuronMetadata = new TmNeuronMetadata(tmWorkspace, name);
        neuronMetadata.setOwnerKey(neuronOwnerKey);
        neuronMetadata.setReaders(tmWorkspace.getReaders());
        neuronMetadata.setWriters(tmWorkspace.getWriters());

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
        TmNeuronUtils.addLinkedGeometricAnnotationsInMemory(nodeParentLinkage, annotations, neuronMetadata, () -> neuronIdGenerator.generateId());

        // Set neuron color
        float[] colorArr = swcData.extractColors();
        if (colorArr != null) {
            Color color = new Color(colorArr[0], colorArr[1], colorArr[2]);
            neuronMetadata.setColor(color);
        }
        return neuronMetadata;
    }

}

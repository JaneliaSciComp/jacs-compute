package org.janelia.jacs2.asyncservice.files;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.janelia.jacs2.asyncservice.dataimport.StorageContentHelper;
import org.janelia.jacs2.asyncservice.lvtservices.HortaDataManager;
import org.janelia.jacs2.dataservice.swc.SWCService;
import org.janelia.jacs2.dataservice.swc.VectorOperator;
import org.janelia.jacsstorage.clients.api.JadeStorageService;
import org.janelia.jacsstorage.clients.api.StorageObject;
import org.janelia.jacsstorage.clients.api.StorageObjectNotFoundException;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.ReverseReference;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.files.SyncedPath;
import org.janelia.model.domain.files.SyncedRoot;
import org.janelia.model.domain.tiledMicroscope.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A file discovery agent which finds Horta samples and creates corresponding TmSample objects.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("hortaDiscoveryAgent")
public class HortaDiscoveryAgent implements FileDiscoveryAgent<TmSample> {

    private static final Logger LOG = LoggerFactory.getLogger(HortaDiscoveryAgent.class);

    @Inject
    private HortaDataManager hortaDataManager;

    @Inject
    private SWCService swcService;


    public TmSample discover(SyncedRoot syncedRoot, Map<String, SyncedPath> currentPaths, JadeObject jadeObject) {

        JadeStorageService jadeStorage = jadeObject.getJadeStorage();
        StorageObject storageObject = jadeObject.getStorageObject();

        if (storageObject.isCollection()) {
            String filepath = storageObject.getAbsolutePath();
            LOG.info("Inspecting potential TM sample directory {}", filepath);

            Path transformPath = Paths.get(storageObject.getAbsolutePath(), "transform.txt");

            if (jadeStorage.exists(storageObject.getLocation(), transformPath.toString())) {
                LOG.info("  Found transform.txt");

                String sampleName = storageObject.getObjectName();

                TmSample sample = new TmSample();
                sample.setAutoSynchronized(true);
                sample.setExistsInStorage(true);
                sample.setName(sampleName);
                sample.setFilepath(filepath);

                if (hasKTX(jadeObject)) {
                    LOG.info("  Found KTX imagery");
                    sample.setLargeVolumeKTXFilepath(storageObject.getAbsolutePath() + "/ktx");
                }
                else {
                    LOG.info("  Could not find KTX files");
                    sample.getFiles().remove(FileType.LargeVolumeKTX);
                }

                if (currentPaths.containsKey(filepath)) {
                    LOG.info("Updating TmSample: "+filepath);
                    TmSample existingSample = (TmSample)currentPaths.get(filepath);
                    try {
                        // Copy discovered properties to existing sample
                        existingSample.setExistsInStorage(true);
                        for(FileType fileType : sample.getFiles().keySet()) {
                            existingSample.getFiles().put(fileType, sample.getFiles().get(fileType));
                        }
                        // Update database
                        hortaDataManager.updateSample(syncedRoot.getOwnerKey(), existingSample);
                        LOG.info("  Updated TM sample: {}", existingSample);
                        sample = existingSample;
                    }
                    catch (Exception e) {
                        LOG.error("  Could not update TM sample "+existingSample, e);
                    }
                }
                else {
                    LOG.info("Found new TmSample: " + sampleName);
                    try {
                        TmSample savedSample = hortaDataManager.createTmSample(syncedRoot.getOwnerKey(), sample);
                        LOG.info("  Created TM sample: {}", savedSample);
                        sample = savedSample;
                    }
                    catch (Exception e) {
                        LOG.error("  Could not create TM sample for " + sampleName, e);
                    }
                }

                // Load neurons into workspaces
                Path neuronsPath = Paths.get(storageObject.getAbsolutePath(), "neurons.json");
                Path deeplinksPath = Paths.get(storageObject.getAbsolutePath(), "deeplinks.json");
                if (jadeStorage.exists(storageObject.getLocation(), neuronsPath.toString())) {
                    try {
                        loadNeurons(syncedRoot.getOwnerKey(), jadeObject, neuronsPath, deeplinksPath, sample);
                    } catch (Exception e) {
                        LOG.error("Error loading neurons from "+neuronsPath, e);
                    }
                }

                return sample;
            }
        }

        return null;
    }

    /**
     * Returns true if the given Storage Object has a subdirectory called "ktx" containing at least one
     * file ending in a ".ktx" extension.
     * @param jadeObject path to search
     * @return true if the path contains ktx files
     */
    private boolean hasKTX(JadeObject jadeObject) {

        JadeStorageService jadeStorage = jadeObject.getJadeStorage();
        StorageObject storageObject = jadeObject.getStorageObject();
        Path ktxPath = Paths.get(storageObject.getAbsolutePath(), "ktx");

        try {
            return jadeStorage.getChildren(storageObject.getLocation(), ktxPath.toString())
                    .stream().anyMatch(k -> k.getAbsolutePath().endsWith(".ktx"));
        }
        catch (StorageObjectNotFoundException e) {
            return false;
        }
    }

    /**
     * Loads the neurons in the given path into a new workspace on the given sample. If a workspace already
     * exists with the correct name then this method returns without making any changes, even if the
     * content of the file has changed since the neurons were originally imported.
     * @param subjectKey owner of the workspace and neurons
     * @param jadeObject jade object representing the folder containing the neurons
     * @param neuronsPath path to the neurons metadata json
     * @param sample sample containing the related imagery
     */
    private void loadNeurons(String subjectKey, JadeObject jadeObject, Path neuronsPath, Path deepLinksPath, TmSample sample) throws IOException {

        LOG.info("  Loading {} as a workspace", neuronsPath);

        // Get existing workspaces owned by the SyncedRoot owner
        Map<String, TmWorkspace> workspacesByName = hortaDataManager.getWorkspaces(subjectKey, sample)
                .stream().filter(s -> s.getOwnerKey().equals(subjectKey))
                .collect(Collectors.toMap(TmWorkspace::getName, Function.identity()));

        LOG.info("Found {} existing workspaces owned by {} for {}", workspacesByName.size(), subjectKey, sample);

        JadeStorageService jadeStorage = jadeObject.getJadeStorage();
        StorageObject storageObject = jadeObject.getStorageObject();

        InputStream content = jadeStorage.getContent(storageObject.getLocation(), neuronsPath.toString());

        ObjectMapper objectMapper = new ObjectMapper();

        // create deep link document to store soma locations
        ArrayNode deeplinkNeuronList = objectMapper.createArrayNode();

        JsonNode root = objectMapper.readTree(content);
        String title = root.get("title").asText();

        TmWorkspace existingWorkspace = workspacesByName.get(title);
        if (existingWorkspace != null) {
            LOG.info("    Workspace already exists: {}", existingWorkspace);
            return;
        }

        VectorOperator externalToInternalConverter = swcService.getExternalToInternalConverter(sample);
        TmWorkspace tmWorkspace = hortaDataManager.createWorkspace(subjectKey, sample, title);
        LOG.info("Created workspace {} for sample {} to load neurons from {}", tmWorkspace, sample, neuronsPath);

        try {
            long neuronCount = 0;

            for (Iterator<Map.Entry<String, JsonNode>> fields = root.get("neurons").fields(); fields.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String neuronBrowserName = entry.getKey();

                JsonNode value = entry.getValue();
                String originalName = value.get("originalName").asText();
                String somaLocation = value.get("somaLocation").asText();

                // Resolve relative paths
                Path folderPath = Paths.get(storageObject.getAbsolutePath());

                TmMappedNeuron mappedNeuron = new TmMappedNeuron();
                mappedNeuron.setName(neuronBrowserName);
                mappedNeuron.setSomaLocation(somaLocation);
                mappedNeuron.setCrossRefInternal(originalName);
                mappedNeuron.setCrossRefNeuronBrowser(neuronBrowserName);

                // Denormalize workspace information, to allow searching of neurons by workspace
                mappedNeuron.setWorkspaceId(tmWorkspace.getId());
                mappedNeuron.setWorkspaceName(tmWorkspace.getName());
                mappedNeuron.setWorkspaceRef(Reference.createFor(tmWorkspace));

                // Load trace of axon, if available
                ObjectNode deepLinkNeuron;
                if (value.has("consensus")) {
                    String consensus = value.get("consensus").asText();
                    String consensusUrl = getPath(folderPath, consensus);
                    TmNeuronMetadata consensusNeuron = swcService.importSWC(
                            consensusUrl,
                            tmWorkspace,
                            neuronBrowserName+" consensus",
                            subjectKey,
                            externalToInternalConverter,
                            sample.getStorageAttributes()
                    );

                    List<TmGeoAnnotation> neuronRoots = consensusNeuron.getRootAnnotations();
                    if (neuronRoots != null && neuronRoots.size()>0) {
                        TmGeoAnnotation mainNeuronRoot = neuronRoots.get(0);
                        deepLinkNeuron = objectMapper.createObjectNode();
                        String deeplink = generateDeepLink(objectMapper, mainNeuronRoot, tmWorkspace.getId());
                        deepLinkNeuron.put("name", neuronBrowserName);
                        deepLinkNeuron.put("originalName", originalName);
                        deepLinkNeuron.put("type", "consensus");
                        deepLinkNeuron.put("deeplinkURL", deeplink);
                        deeplinkNeuronList.add(deepLinkNeuron);
                    }

                    LOG.debug("  Loaded consensus SWC as {}", consensusNeuron);
                    mappedNeuron.addNeuronRef(Reference.createFor(consensusNeuron));
                }

                // Load dendrites, if available
                if (value.has("dendrite")) {
                    String dendrite = value.get("dendrite").asText();
                    String dendriteUrl = getPath(folderPath, dendrite);
                    TmNeuronMetadata dendriteNeuron = swcService.importSWC(
                            dendriteUrl,
                            tmWorkspace,
                            neuronBrowserName+" dendrite",
                            subjectKey,
                            externalToInternalConverter,
                            sample.getStorageAttributes()
                    );
                    List<TmGeoAnnotation> neuronRoots = dendriteNeuron.getRootAnnotations();
                    if (neuronRoots != null && neuronRoots.size()>0) {
                        TmGeoAnnotation mainNeuronRoot = neuronRoots.get(0);
                        deepLinkNeuron = objectMapper.createObjectNode();
                        String deeplink = generateDeepLink(objectMapper, mainNeuronRoot, tmWorkspace.getId());
                        deepLinkNeuron.put("name", neuronBrowserName);
                        deepLinkNeuron.put("originalName", originalName);
                        deepLinkNeuron.put("type", "dendrite");
                        deepLinkNeuron.put("deeplinkURL", deeplink);
                        deeplinkNeuronList.add(deepLinkNeuron);
                    }
                    LOG.debug("  Loaded dendrite SWC as {}", dendriteNeuron);
                    mappedNeuron.addNeuronRef(Reference.createFor(dendriteNeuron));
                }

                if (mappedNeuron.getNeuronRefs().isEmpty()) {
                    LOG.warn("  Could not find SWC files to load for neuron {}", neuronBrowserName);
                }
                else {
                    TmMappedNeuron savedNeuron = hortaDataManager.createMappedNeuron(subjectKey, mappedNeuron);
                    LOG.info("  Loaded neuron {} as {}", neuronBrowserName, savedNeuron);
                    neuronCount++;
                }
            }
            InputStream deepLinkStream = new ByteArrayInputStream(objectMapper.writeValueAsString(deeplinkNeuronList)
                    .getBytes(StandardCharsets.UTF_8));
            jadeStorage.setContent(storageObject.getLocation(), deepLinksPath.toString(), deepLinkStream);

            // Denormalize information so that it can be traversed during indexing,
            // to allow searching of workspaces by neuron name
            ReverseReference mappedNeuronsRef = new ReverseReference();
            mappedNeuronsRef.setReferringClassName(TmMappedNeuron.class.getName());
            mappedNeuronsRef.setReferenceAttr("workspaceId");
            mappedNeuronsRef.setReferenceId(tmWorkspace.getId());
            mappedNeuronsRef.setCount(neuronCount);
            tmWorkspace.setMappedNeurons(mappedNeuronsRef);
            hortaDataManager.updateWorkspace(subjectKey, tmWorkspace);
        }
        catch (Exception e) {
            LOG.error("Error importing "+neuronsPath+". Attempting rollback.", e);
            hortaDataManager.removeWorkspace(subjectKey, tmWorkspace);
        }
    }


    /**
     * takes a deep link set of view parameters and URL-escapes them
     * @param value
     * @return
     * @throws UnsupportedEncodingException
     */
    private String encodeValue(String value) throws UnsupportedEncodingException {
        return URLEncoder.encode(value, StandardCharsets.UTF_8.toString());
    }

    /**
     *
     * @param mapper jackson object mapper
     * @param rootNode soma Node for the neuron
     * @param workspaceId - current workspace being synchronized
     * @return
     */
    private String generateDeepLink (ObjectMapper mapper, TmGeoAnnotation rootNode, Long workspaceId) {
        Map<String, Object> deepLinkMap = new HashMap<>();
        deepLinkMap.put("workspace", workspaceId);
        deepLinkMap.put("viewFocusX", rootNode.getX());
        deepLinkMap.put("viewFocusY", rootNode.getY());
        deepLinkMap.put("viewFocusZ", rootNode.getZ());
        deepLinkMap.put("viewZoom", 100);
        String encodedURL = deepLinkMap.keySet().stream()
                .map(key -> {
                    try {
                        String jsonVal = mapper.writeValueAsString(deepLinkMap.get(key));
                        return key + "=" + encodeValue(jsonVal);
                    } catch (UnsupportedEncodingException | JsonProcessingException e) {
                        e.printStackTrace();
                        return "";
                    }
                })
                .collect(Collectors.joining("&", "deeplink:", ""));
        return encodedURL;
    }
    /**
     * Returns the full absolute path to the SWC, given a current working directory. If the given swcPath is
     * absolute, just return it. If it's relative, resolve it relative to the cwd.
     * @param cwd current working directory
     * @param swcPath path to swc (absolute or relative to cwd)
     * @return absolute path to swc
     */
    private String getPath(Path cwd, String swcPath) {
        return swcPath.startsWith("/")
                ? swcPath
                : cwd.resolve(swcPath).normalize().toString();
    }
}

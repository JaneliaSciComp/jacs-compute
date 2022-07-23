package org.janelia.jacs2.asyncservice.files;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacsstorage.newclient.*;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.SyncedRootDao;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.files.DiscoveryAgentType;
import org.janelia.model.domain.files.SyncedPath;
import org.janelia.model.domain.files.SyncedRoot;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This service searches the given path using a set of discovery agents which synchronize paths to the database.
 *
 * Use the SyncedPathResource web API to define a SyncedRoot before invoking this service.
 */
@Named("syncedRoot")
public class SyncedRootProcessor extends AbstractServiceProcessor<Long> {

    private static final Logger LOG = LoggerFactory.getLogger(SyncedRootProcessor.class);

    static class SyncedRootArgs extends ServiceArgs {
        @Parameter(names = "-syncedRootId", description = "Id of existing SyncedRoot object.", required = true)
        Long syncedRootId;
        @Parameter(names = "-dryRun", description = "Process the path normally, but don't invoke any discovery agents.", arity = 1)
        boolean dryRun = false;
        SyncedRootArgs() {
            super("Service that synchronizes file present in JADE to the database");
        }
    }

    private final StorageService storageService;
    private final Instance<FileDiscoveryAgent<?>> agentSource;
    private final SyncedRootDao syncedRootDao;
    private final LegacyDomainDao legacyDomainDao;

    @Inject
    public SyncedRootProcessor(ServiceComputationFactory computationFactory,
                               JacsServiceDataPersistence jacsServiceDataPersistence,
                               @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                               @PropertyValue(name = "StorageService.URL") String masterStorageServiceURL,
                               @PropertyValue(name = "StorageService.ApiKey") String storageServiceApiKey,
                               @Any Instance<FileDiscoveryAgent<?>> agentSource,
                               SyncedRootDao syncedRootDao,
                               LegacyDomainDao legacyDomainDao,
                               Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.storageService = new StorageService(masterStorageServiceURL, storageServiceApiKey);
        this.agentSource = agentSource;
        this.syncedRootDao = syncedRootDao;
        this.legacyDomainDao = legacyDomainDao;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SyncedRootProcessor.class, new SyncedRootArgs());
    }

    @Override
    public ServiceResultHandler<Long> getResultHandler() {
        return new AbstractAnyServiceResultHandler<Long>() {

            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            @Override
            public Long getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<Long>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<Long>> process(JacsServiceData jacsServiceData) {
        SyncedRootArgs args = getArgs(jacsServiceData);
        String authToken = ResourceHelper.getAuthToken(jacsServiceData.getResources());
        SyncedRoot syncedRoot = getSyncedRoot(args.syncedRootId);

        JadeStorageService jadeStorage = new JadeStorageService(
                storageService, syncedRoot.getOwnerKey(), authToken);

        StorageLocation storageLocation = jadeStorage.getStorageLocationByPath(syncedRoot.getFilepath());
        if (storageLocation == null) {
            throw new ComputationException(jacsServiceData, "Could not find storage location for path " + syncedRoot.getFilepath());
        }

        StorageObject rootMetadata;
        try {
            rootMetadata = jadeStorage.getMetadata(storageLocation, syncedRoot.getFilepath());
        }
        catch (StorageObjectNotFoundException e) {
            throw new ComputationException(jacsServiceData, "Could not find metadata for " + storageLocation.getStorageURL() + " path " + syncedRoot.getFilepath());
        }

        LOG.info("Found {} in {}", syncedRoot, storageLocation);
        JadeObject rootObject = new JadeObject(jadeStorage, rootMetadata);

        List<DomainObject> syncedPaths = new ArrayList<>();
        List<FileDiscoveryAgent<?>> agents = new ArrayList<>();
        if (!args.dryRun) {
            for (FileDiscoveryAgent<?> agent : agentSource) {
                Named namedAnnotation = agent.getClass().getAnnotation(Named.class);
                String serviceName = namedAnnotation.value();
                DiscoveryAgentType agentType = DiscoveryAgentType.valueOf(serviceName);
                if (syncedRoot.getDiscoveryAgents().contains(agentType)) {
                    logger.info("Using discovery service: {}", serviceName);
                    agents.add(agent);
                }
                syncedPaths.addAll(legacyDomainDao.getUserDomainObjects(syncedRoot.getOwnerKey(), agentType.getDomainObjectClass()));
            }
        }
        else {
            logger.info("Service running in dry run mode. No discovery agents will be called.");
        }

        // Build of map of current paths that can be reused
        Map<String, SyncedPath> currentPaths = syncedPaths
                .stream().map(d -> (SyncedPath)d) // all SyncedRoot children must extend SyncedPath
                .collect(Collectors.toMap(SyncedPath::getFilepath, Function.identity()));

        // Initialize all database paths to existsInStorage=false
        for (String filepath : currentPaths.keySet()) {
            SyncedPath syncedPath = currentPaths.get(filepath);
            syncedPath.setExistsInStorage(false);
        }

        // Process all paths in storage
        List<DomainObject> newChildren = walkStorage(syncedRoot, currentPaths, rootObject, agents, syncedRoot.getDepth(), "");

        // Update the root's children
        if (!args.dryRun) {
            syncedRootDao.updateChildren(syncedRoot.getOwnerKey(), syncedRoot, DomainUtils.getReferences(newChildren));
        }

        // Any database paths in this directory that are still existsInStorage=false should have that status persisted
        for (String filepath : currentPaths.keySet()) {
            SyncedPath syncedPath = currentPaths.get(filepath);
            if (filepath.startsWith(syncedRoot.getFilepath())) {
                if (!syncedPath.isExistsInStorage()) {
                    LOG.info("Path no longer exists in storage: {}", syncedPath.getFilepath());
                    if (!args.dryRun) {
                        try {
                            legacyDomainDao.updateProperty(syncedRoot.getOwnerKey(), syncedPath.getClass(),
                                    syncedPath.getId(), "existsInStorage", false);
                        } catch (Exception e) {
                            throw new ComputationException(jacsServiceData, "Could not update " + syncedPath);
                        }
                    }
                }
            }
        }

        return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData));
    }

    private List<DomainObject> walkStorage(SyncedRoot syncedRoot, Map<String, SyncedPath> currentPaths, JadeObject jadeObject,
                             List<FileDiscoveryAgent<?>> agents, int levels, String indent) {
        try {
            List<DomainObject> discovered = new ArrayList<>();
            for (JadeObject child : jadeObject.getChildren()) {
                StorageObject storageObject = child.getStorageObject();
                logger.info(indent+"{} -> {}", storageObject.getObjectName(), storageObject.getAbsolutePath());
                logger.debug(indent+"      {}", child);
                for (FileDiscoveryAgent<? extends DomainObject> agent : agents) {
                    DomainObject discoveredObject = agent.discover(syncedRoot, currentPaths, child);
                    if (discoveredObject != null) {
                        discovered.add(discoveredObject);
                        break; // a given path can only map to a single discovered object
                    }
                }
                if (levels > 1 && storageObject.isCollection()) {
                    // Recurse into the folder hierarchy
                    discovered.addAll(walkStorage(syncedRoot, currentPaths, child, agents, levels - 1, indent+"  "));
                }
            }
            return discovered;
        }
        catch (StorageObjectNotFoundException e) {
            throw new IllegalStateException("Storage object disappeared mysteriously: "
                    + jadeObject.getStorageObject().getAbsolutePath());
        }
    }

    private SyncedRoot getSyncedRoot(Long syncedRootId) {
        SyncedRoot syncedRoot = syncedRootDao.findById(syncedRootId);
        if (syncedRoot==null) {
            throw new IllegalArgumentException("Cannot find SyncedRoot#"+syncedRootId);
        }
        return syncedRoot;
    }

    private SyncedRootArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SyncedRootArgs());
    }
}

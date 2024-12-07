package org.janelia.jacs2.asyncservice.files;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ResourceHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacsstorage.clients.api.JadeStorageAttributes;
import org.janelia.jacsstorage.clients.api.JadeStorageService;
import org.janelia.jacsstorage.clients.api.StorageLocation;
import org.janelia.jacsstorage.clients.api.StorageObject;
import org.janelia.jacsstorage.clients.api.StorageObjectNotFoundException;
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

/**
 * This service searches the given path using a set of discovery agents which synchronize paths to the database.
 * <p>
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

    private final ExecutorService executorService;
    private final String masterStorageServiceURL;
    private final String storageServiceApiKey;
    private final Instance<FileDiscoveryAgent<?>> agentSource;
    private final SyncedRootDao syncedRootDao;
    private final LegacyDomainDao legacyDomainDao;

    @Inject
    public SyncedRootProcessor(ServiceComputationFactory computationFactory,
                               JacsServiceDataPersistence jacsServiceDataPersistence,
                               @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                               @PropertyValue(name = "StorageService.URL") String masterStorageServiceURL,
                               @PropertyValue(name = "StorageService.ApiKey") String storageServiceApiKey,
                               @JacsDefault ExecutorService executorService,
                               @Any Instance<FileDiscoveryAgent<?>> agentSource,
                               SyncedRootDao syncedRootDao,
                               LegacyDomainDao legacyDomainDao,
                               Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.executorService = executorService;
        this.masterStorageServiceURL = masterStorageServiceURL;
        this.storageServiceApiKey = storageServiceApiKey;
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
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<Long>() {
                });
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<Long>> process(JacsServiceData jacsServiceData) {
        SyncedRootArgs args = getArgs(jacsServiceData);
        String authToken = ResourceHelper.getAuthToken(jacsServiceData.getResources());
        SyncedRoot syncedRoot = getSyncedRoot(args.syncedRootId);

        logger.info("Starting refresh for {}", syncedRoot);

        JadeStorageService jadeStorage = new JadeStorageService(masterStorageServiceURL, storageServiceApiKey, syncedRoot.getOwnerKey(), authToken);
        JadeStorageAttributes jadeStorageAttributes = new JadeStorageAttributes()
                .setAttributeValue("AccessKey", jacsServiceData.getDictionaryArgAsString("Storage.AccessKey"))
                .setAttributeValue("SecretKey", jacsServiceData.getDictionaryArgAsString("Storage.SecretKey"));
        StorageLocation storageLocation = jadeStorage.getStorageLocationByPath(syncedRoot.getFilepath(), jadeStorageAttributes);
        if (storageLocation == null) {
            throw new ComputationException(jacsServiceData, "Could not find storage location for path " + syncedRoot.getFilepath());
        }

        StorageObject rootMetadata;
        try {
            rootMetadata = jadeStorage.getMetadata(storageLocation, syncedRoot.getFilepath(), true);
        } catch (StorageObjectNotFoundException e) {
            throw new ComputationException(jacsServiceData, "Could not find metadata for " + storageLocation.getStorageURL() + " path " + syncedRoot.getFilepath());
        }

        LOG.info("Found {} in {}", syncedRoot, storageLocation);
        JadeObject rootObject = new JadeObject(jadeStorage, rootMetadata);

        Map<String, SyncedPath> currentPaths = new HashMap<>();
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
                for (DomainObject domainObject : legacyDomainDao.getUserDomainObjects(syncedRoot.getOwnerKey(), agentType.getDomainObjectClass())) {
                    SyncedPath syncedPath = (SyncedPath) domainObject;
                    // We only touch auto-synchronized paths
                    if (syncedPath.isAutoSynchronized()) {
                        if (currentPaths.containsKey(syncedPath.getFilepath())) {
                            SyncedPath firstSyncedPath = currentPaths.get(syncedPath.getFilepath());
                            logger.info("  Another object already implements {}: {} and {}", syncedPath.getFilepath(), firstSyncedPath, syncedPath);
                        } else {
                            currentPaths.put(syncedPath.getFilepath(), syncedPath);
                        }
                    }
                }
            }
        } else {
            logger.info("Service running in dry run mode. No discovery agents will be called.");
        }

        // Initialize all database paths to existsInStorage=false
        for (String filepath : currentPaths.keySet()) {
            SyncedPath syncedPath = currentPaths.get(filepath);
            syncedPath.setExistsInStorage(false);
        }

        // Walk all paths in storage and spin off agents to process them
        List<Future<DomainObject>> futures =
                walkStorage(syncedRoot, currentPaths, rootObject, agents, syncedRoot.getDepth(), "");

        // Wait for all agents to finish
        List<DomainObject> newChildren = new ArrayList<>();
        for (Future<DomainObject> future : futures) {
            try {
                newChildren.add(future.get(1, TimeUnit.HOURS));
            } catch (InterruptedException e) {
                logger.info("Discovery agent was interrupted", e);
            } catch (ExecutionException e) {
                logger.info("Discovery agent threw an exception", e);
            } catch (TimeoutException e) {
                logger.info("Discovery agent timed out after 1 hour", e);
            }
        }

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
                                    syncedPath.getId(), "existsInStorage", false, boolean.class);
                        } catch (Exception e) {
                            logger.error("Error updating " + syncedPath, e);
                            throw new ComputationException(jacsServiceData, "Could not update " + syncedPath);
                        }
                    }
                }
            }
        }

        logger.info("Completed refresh for {}", syncedRoot);

        return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData));
    }

    private List<Future<DomainObject>> walkStorage(SyncedRoot syncedRoot,
                                                   Map<String, SyncedPath> currentPaths,
                                                   JadeObject jadeObject,
                                                   List<FileDiscoveryAgent<?>> agents,
                                                   int levels,
                                                   String indent) {
        List<Future<DomainObject>> futures = new ArrayList<>();
        try {
            for (JadeObject child : jadeObject.getSubdirs()) {
                StorageObject storageObject = child.getStorageObject();
                logger.debug(indent + "{} -> {}", storageObject.getObjectName(), storageObject.getAbsolutePath());
                logger.debug(indent + "      {}", child);

                futures.add(executorService.submit(() -> {
                    DomainObject discoveredObject = null;
                    for (FileDiscoveryAgent<? extends DomainObject> agent : agents) {
                        discoveredObject = agent.discover(syncedRoot, currentPaths, child);
                        if (discoveredObject != null) {
                            break; // a given path can only map to a single discovered object
                        }
                    }
                    return discoveredObject;
                }));

                if (levels > 1 && storageObject.isCollection()) {
                    // Recurse into the folder hierarchy
                    futures.addAll(walkStorage(syncedRoot, currentPaths, child, agents, levels - 1, indent + "  "));
                }
            }
            return futures;
        } catch (StorageObjectNotFoundException e) {
            throw new IllegalStateException("Storage object disappeared mysteriously: "
                    + jadeObject.getStorageObject().getAbsolutePath());
        }
    }

    private SyncedRoot getSyncedRoot(Long syncedRootId) {
        SyncedRoot syncedRoot = syncedRootDao.findById(syncedRootId);
        if (syncedRoot == null) {
            throw new IllegalArgumentException("Cannot find SyncedRoot#" + syncedRootId);
        }
        return syncedRoot;
    }

    private SyncedRootArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SyncedRootArgs());
    }
}

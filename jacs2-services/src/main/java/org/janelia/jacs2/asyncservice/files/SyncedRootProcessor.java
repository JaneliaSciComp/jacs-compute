package org.janelia.jacs2.asyncservice.files;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.dataimport.BetterStorageHelper;
import org.janelia.jacs2.asyncservice.dataimport.StorageContentObject;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.access.domain.dao.SyncedPathDao;
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
    private final Instance<FileDiscoveryAgent> agentSource;
    private final SyncedPathDao syncedPathDao;

    @Inject
    public SyncedRootProcessor(ServiceComputationFactory computationFactory,
                               JacsServiceDataPersistence jacsServiceDataPersistence,
                               @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                               @Any Instance<FileDiscoveryAgent> agentSource,
                               StorageService storageService,
                               SyncedPathDao syncedPathDao,
                               Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.agentSource = agentSource;
        this.storageService = storageService;
        this.syncedPathDao = syncedPathDao;
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

        SyncedPath syncedPath = syncedPathDao.findById(args.syncedRootId);
        if (syncedPath==null) {
            throw new IllegalArgumentException("Cannot find SyncedRoot#"+args.syncedRootId);
        }
        if (!(syncedPath instanceof SyncedRoot)) {
            throw new IllegalArgumentException("SyncedPath#"+args.syncedRootId+" is not a SyncedRoot");
        }

        SyncedRoot syncedRoot = (SyncedRoot)syncedPath;

        BetterStorageHelper storageContentHelper = new BetterStorageHelper(
                storageService, jacsServiceData.getOwnerKey(), authToken);

        StorageContentObject storageLocation = storageContentHelper
                .getStorageObjectByPath(syncedRoot.getFilepath())
                .orElseThrow(() -> new ComputationException(jacsServiceData, "Could not find any storage for "+syncedRoot+" path " + syncedRoot.getFilepath()));
        LOG.info("Found {} in {}", syncedRoot, storageLocation);

        List<FileDiscoveryAgent> agents = new ArrayList<>();
        if (!args.dryRun) {
            for (FileDiscoveryAgent agent : agentSource) {
                Named namedAnnotation = agent.getClass().getAnnotation(Named.class);
                String serviceName = namedAnnotation.value();
                DiscoveryAgentType agentType = DiscoveryAgentType.valueOf(serviceName);
                if (syncedRoot.getDiscoveryAgents().contains(agentType)) {
                    logger.info("Using discovery service: {}", serviceName);
                    agents.add(agent);
                }
            }
        }
        else {
            logger.info("Service running in dry run mode. No discovery agents will be called.");
        }

        walkStorage(syncedRoot, storageLocation, agents, syncedRoot.getDepth(), "");

        return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData));
    }

    private void walkStorage(SyncedRoot syncedRoot, StorageContentObject location,
                             List<FileDiscoveryAgent> agents, int levels, String indent) {
        location.getHelper().listContent(location)
                .forEach(child -> {
                    logger.info(indent+"{} -> {}", child.getName(), child.getAbsolutePath());
                    logger.info(indent+"      {}", child);
                    if (child.isCollection()) {
                        if (levels > 1) {
                            walkStorage(syncedRoot, child, agents, levels - 1, indent+"  ");
                        }
                    }
                    for (FileDiscoveryAgent agent : agents) {
                        agent.discover(syncedRoot, child);
                    }
                });
    }

    private SyncedRootArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SyncedRootArgs());
    }
}

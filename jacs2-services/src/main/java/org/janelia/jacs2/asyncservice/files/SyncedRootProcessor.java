package org.janelia.jacs2.asyncservice.files;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.dataimport.BetterStorageHelper;
import org.janelia.jacs2.asyncservice.dataimport.StorageObject;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.access.domain.dao.SyncedPathDao;
import org.janelia.model.domain.files.SyncedPath;
import org.janelia.model.domain.files.SyncedRoot;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * This service searches the given path using a set of discovery agents which synchronize paths to the database.
 * It takes a SyncedRoot id as input
 */
@Named("syncedRoot")
public class SyncedRootProcessor extends AbstractServiceProcessor<Long> {

    static class SyncedRootArgs extends ServiceArgs {
        @Parameter(names = "-syncedRootId", description = "Id of existing SyncedRoot object.")
        Long syncedRootId;
        @Parameter(names = "-discoveryAgents", description = "List of full-qualified class names which implement the DiscoveryAgent interface", required = true)
        Set<String> discoveryAgents;
        @Parameter(names = "-levels", description = "Levels of storage hierarchy to traverse for discovery.")
        Integer levels = 2;
        @Parameter(names = "-ownerKey", description = "Owner of the created objects. If not set the owner is the service caller.")
        String ownerKey;
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
        String objectOwnerKey = StringUtils.isBlank(args.ownerKey) ? jacsServiceData.getOwnerKey() : args.ownerKey.trim();

        SyncedPath synchedPath = syncedPathDao.findById(args.syncedRootId);
        if (synchedPath==null) {
            throw new IllegalArgumentException("Cannot find SyncedRoot#"+args.syncedRootId);
        }
        if (!(synchedPath instanceof SyncedRoot)) {
            throw new IllegalArgumentException("SyncedPath#"+args.syncedRootId+" is not a SyncedRoot");
        }

        SyncedRoot syncedRoot = (SyncedRoot)synchedPath;

        BetterStorageHelper storageContentHelper = new BetterStorageHelper(
                storageService, jacsServiceData.getOwnerKey(), authToken);

        StorageObject storageLocation = storageContentHelper
                .lookupPath(syncedRoot.getFilepath())
                .orElseThrow(() -> new ComputationException(jacsServiceData, "Could not find any storage for "+syncedRoot+" path " + syncedRoot.getFilepath()));

        List<FileDiscoveryAgent> agents = new ArrayList<>();
        if (!args.dryRun) {
            for (FileDiscoveryAgent agent : agentSource) {
                Named namedAnnotation = agent.getClass().getAnnotation(Named.class);
                String serviceName = namedAnnotation.value();
                if (args.discoveryAgents.contains(serviceName)) {
                    logger.info("Using discovery service: {}", serviceName);
                    agents.add(agent);
                }
            }
        }
        else {
            logger.info("Service running in dry run mode. No discovery agents will be called.");
        }

        walkStorage(objectOwnerKey, syncedRoot, storageLocation, agents,args.levels, "");

        return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData));
    }

    private void walkStorage(String objectOwnerKey, SyncedRoot syncedRoot, StorageObject location,
                             List<FileDiscoveryAgent> agents, int levels, String indent) {
        location.getHelper().listContent(location)
                .forEach(child -> {
                    logger.info(indent+"{} -> {}", child.getName(), child.getAbsolutePath());
                    if (child.isCollection()) {
                        if (levels > 1) {
                            walkStorage(objectOwnerKey, syncedRoot, child, agents, levels - 1, indent+"  ");
                        }
                    }
                    for (FileDiscoveryAgent agent : agents) {
                        agent.discover(objectOwnerKey, syncedRoot, child);
                    }
                });
    }

    private SyncedRootArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SyncedRootArgs());
    }
}

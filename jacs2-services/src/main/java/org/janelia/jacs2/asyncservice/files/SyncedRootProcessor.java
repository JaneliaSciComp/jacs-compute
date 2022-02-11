package org.janelia.jacs2.asyncservice.files;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.dataimport.BetterStorageHelper;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.storage.StorageService;
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
 */
@Named("syncedRoot")
public class SyncedRootProcessor extends AbstractServiceProcessor<Long> {

    static class SyncedRootArgs extends ServiceArgs {
        @Parameter(names = "-path", description = "Path to search", required = true)
        String path;
        @Parameter(names = "-discoveryAgents", description = "List of full-qualified class names which implement the DiscoveryAgent interface", required = true)
        Set<String> discoveryAgents;
        @Parameter(names = "-syncedRootId", description = "Id of existing SyncedRoot to update")
        Long syncedRootId;
        @Parameter(names = "-levels", description = "Levels of storage hierarchy to traverse")
        Integer levels = 2;
        @Parameter(names = "-ownerKey", description = "Owner of the created objects. If not set the owner is the service caller.")
        String ownerKey;
        @Parameter(names = "-dryRun", description = "Process everything normally but forgo persisting to the database", arity = 1)
        boolean dryRun = false;
        SyncedRootArgs() {
            super("Service that synchronizes file present in JADE to the database");
        }
    }

    private final StorageService storageService;
    private final Instance<FileDiscoveryAgent> agentSource;

    @Inject
    public SyncedRootProcessor(ServiceComputationFactory computationFactory,
                               JacsServiceDataPersistence jacsServiceDataPersistence,
                               @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                               @Any Instance<FileDiscoveryAgent> agentSource,
                               StorageService storageService,
                               Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.agentSource = agentSource;
        this.storageService = storageService;
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

        BetterStorageHelper storageContentHelper = new BetterStorageHelper(storageService, jacsServiceData.getOwnerKey(), authToken);
        BetterStorageHelper.StorageLocation storageLocation = storageContentHelper
                .lookupPath(args.path)
                .orElseThrow(() -> new ComputationException(jacsServiceData, "Could not find any storage for path " + args.path));

        List<FileDiscoveryAgent> agents = new ArrayList<>();
        for(FileDiscoveryAgent agent : agentSource) {
            Named namedAnnotation = agent.getClass().getAnnotation(Named.class);
            String serviceName = namedAnnotation.value();
            if (args.discoveryAgents.contains(serviceName)) {
                logger.info("Using discovery service: {}", serviceName);
                agents.add(agent);
            }
        }

        walkStorage(storageLocation, agents,args.levels, "");

        return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData));
    }

    private void walkStorage(BetterStorageHelper.StorageLocation location, List<FileDiscoveryAgent> agents, int levels, String indent) {
        location.getHelper().listContent(location)
                .forEach(storageObject -> {
                    logger.info(indent+"{} -> {}", storageObject.getName(), storageObject.getAbsolutePath());
                    if (storageObject.isDirectory()) {
                        BetterStorageHelper.StorageLocation childLocation = storageObject.toStorageLocation();
                        if (levels > 1) {
                            walkStorage(childLocation, agents, levels - 1, indent+"  ");
                        }
                    }
                    for (FileDiscoveryAgent agent : agents) {
                        //agent.discover();
                    }
                });
    }

    private SyncedRootArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SyncedRootArgs());
    }

}

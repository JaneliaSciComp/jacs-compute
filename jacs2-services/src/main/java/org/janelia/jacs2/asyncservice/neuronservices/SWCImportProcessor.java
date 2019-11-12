package org.janelia.jacs2.asyncservice.neuronservices;

import java.util.LinkedHashSet;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.swc.SWCService;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Named("swcImport")
public class SWCImportProcessor extends AbstractServiceProcessor<Long> {

    static class SWCImportArgs extends ServiceArgs {
        @Parameter(names = "-sampleId", description = "ID of the sample to be imported", required = true)
        Long sampleId;
        @Parameter(names = "-workspace", description = "Workspace name", required = true)
        String workspace;
        @Parameter(names = "-swcDirName", description = "SWC directory name", required = true)
        String swcDirName;
        @Parameter(names = "-workspaceOwner", description = "If not set the workspace owner is the service caller")
        String workspaceOwnerKey;
        @Parameter(names = "-neuronsOwner", description = "If not set the neurons owner is the service caller")
        String neuronsOwnerKey;
        @Parameter(names = "-firstSWCFile", description = "The index of the first SWC file to be imported")
        long firsSWCFile = 0;
        @Parameter(names = "-orderSWCs", description = "If set, import SWCs in order", arity = 1)
        boolean orderSWCs = false;
        SWCImportArgs() {
            super("Service that imports an SWC file into a workspace");
        }
    }

    private final SWCService swcService;

    @Inject
    SWCImportProcessor(ServiceComputationFactory computationFactory,
                       JacsServiceDataPersistence jacsServiceDataPersistence,
                       @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                       SWCService swcService,
                       Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.swcService = swcService;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SWCImportProcessor.class, new SWCImportArgs());
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
        SWCImportArgs args = getArgs(jacsServiceData);
        String workspaceOwnerKey;
        String neuronOwnerKey;
        Set<String> accessUsers = new LinkedHashSet<>();
        if (StringUtils.isBlank(args.workspaceOwnerKey)) {
            workspaceOwnerKey = jacsServiceData.getOwnerKey();
        } else {
            workspaceOwnerKey = args.workspaceOwnerKey.trim();
        }
        if (StringUtils.isBlank(args.neuronsOwnerKey)) {
            neuronOwnerKey = workspaceOwnerKey; // default to workspace owner
        } else {
            neuronOwnerKey = args.neuronsOwnerKey.trim();
            accessUsers.add(neuronOwnerKey);
        }
        accessUsers.remove(workspaceOwnerKey); // if the neuron owner and the workspace owner are the same there's no need for this
        return computationFactory
                .newCompletedComputation(swcService.importSWCFolder(args.swcDirName,
                        args.sampleId, args.workspace,
                        workspaceOwnerKey, neuronOwnerKey,
                        ImmutableList.copyOf(accessUsers),
                        args.firsSWCFile, args.orderSWCs))
                .thenApply(tmWorkspace -> updateServiceResult(jacsServiceData, tmWorkspace.getId()));
    }

    private SWCImportArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SWCImportArgs());
    }

}

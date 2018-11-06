package org.janelia.jacs2.asyncservice.neuronservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
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

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Named("swcImport")
public class SWCImportProcessor extends AbstractServiceProcessor<Long> {

    static class SWCImportArgs extends ServiceArgs {
        @Parameter(names = "-sampleId", description = "ID of the sample to be imported", required = true)
        Long sampleId;
        @Parameter(names = "-workspace", description = "Workspace name", required = true)
        String workspace;
        @Parameter(names = "-swcDirName", description = "SWC directory name", required = true)
        String swcDirName;
        @Parameter(names = "-withSystemOwner", description = "If set mouselight group user is the owner of the imported neurons")
        boolean withSystemOwner;
        SWCImportArgs() {
            super("Service that imports an SWC file into a workspace");
        }
    }

    private static final String SYSTEM_OWNER_KEY = "group:mouselight";

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
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @SuppressWarnings("unchecked")
            @Override
            public Long collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<Long> result = (JacsServiceResult<Long>)depResults;
                return result.getResult();
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
        String neuronOwnerKey;
        List<String> accessUsers;
        if (args.withSystemOwner) {
            neuronOwnerKey = SYSTEM_OWNER_KEY;
            accessUsers = Arrays.asList(SYSTEM_OWNER_KEY);
        } else {
            neuronOwnerKey = jacsServiceData.getOwnerKey();
            accessUsers = Collections.emptyList();
        }
        return computationFactory
                .newCompletedComputation(swcService.importSWCFolder(args.swcDirName,
                        args.sampleId, neuronOwnerKey, args.workspace, jacsServiceData.getOwnerKey(), accessUsers))
                .thenApply(tmWorkspace -> updateServiceResult(jacsServiceData, tmWorkspace.getId()));
    }

    private SWCImportArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SWCImportArgs());
    }

}

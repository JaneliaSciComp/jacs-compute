package org.janelia.jacs2.asyncservice.lvtservices;

import javax.inject.Inject;
import javax.inject.Named;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * The LVT (Large Volume Tools) Data Import first converts the input data into an Octree using OctreeCreator
 * and then runs KTXCreator to create the KTX tiles for Horta.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("lvDataImport")
public class LVDataImport extends AbstractServiceProcessor<LVResult> {

    static class LVDataImportArgs extends LVArgs {
        @Parameter(names = "-voxelSize", description = "Voxel size (in 'x,y,z' format)", required = true)
        String voxelSize;
        @Parameter(names = "-subtreeLengthForSubjobSplitting", description = "The subtree length considered for job splitting")
        Integer subtreeLengthForSubjobSplitting = 5;
    }

    private final WrappedServiceProcessor<OctreeCreator, OctreeResult> octreeCreator;
    private final WrappedServiceProcessor<KTXCreator,OctreeResult> ktxCreator;

    @Inject
    LVDataImport(ServiceComputationFactory computationFactory,
                 JacsServiceDataPersistence jacsServiceDataPersistence,
                 @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                 OctreeCreator octreeCreator,
                 KTXCreator ktxCreator,
                 Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.octreeCreator = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, octreeCreator);
        this.ktxCreator = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, ktxCreator);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LVDataImport.class, new LVDataImportArgs());
    }

    @Override
    public ServiceResultHandler<LVResult> getResultHandler() {
        return new AbstractAnyServiceResultHandler<LVResult>() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            @Override
            public LVResult getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<LVResult>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<LVResult>> process(JacsServiceData jacsServiceData) {
        LVDataImportArgs args = getArgs(jacsServiceData);

        final String inputTiffDir = args.inputDir;
        final String octreeDir = args.outputDir;
        final String ktxDir = StringUtils.appendIfMissing(octreeDir, "/") + "ktx";
        final int levels = args.levels;
        final String voxelSize = args.voxelSize;

        return octreeCreator.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Create octree")
                        .build(),
                new ServiceArg("-inputDir", inputTiffDir),
                new ServiceArg("-outputDir", octreeDir),
                new ServiceArg("-levels", levels),
                new ServiceArg("-voxelSize", voxelSize))
                .thenCompose((JacsServiceResult<OctreeResult> octreeResult) ->
                        ktxCreator.process(new ServiceExecutionContext.Builder(jacsServiceData)
                                .description("Create ktx tiles")
                                .waitFor(octreeResult.getJacsServiceData())
                                .build(),
                        new ServiceArg("-inputDir", octreeDir),
                        new ServiceArg("-outputDir", ktxDir),
                        new ServiceArg("-levels", levels),
                        new ServiceArg("-subtreeLengthForSubjobSplitting", args.subtreeLengthForSubjobSplitting.toString())
                ))
                .thenApply((JacsServiceResult<OctreeResult> ktxResult) -> {
                    LVResult lvResult = new LVResult();
                    lvResult.setBaseTiffPath(octreeDir);
                    lvResult.setBaseKtxPath(ktxDir);
                    lvResult.setLevels(levels);
                    return updateServiceResult(jacsServiceData, lvResult);
                })
                ;
    }


    private LVDataImportArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LVDataImportArgs());
    }
}

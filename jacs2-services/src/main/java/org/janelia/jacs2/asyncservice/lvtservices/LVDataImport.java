package org.janelia.jacs2.asyncservice.lvtservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.util.concurrent.UncheckedExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.domain.DomainConstants;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.enums.FileType;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.model.domain.workspace.TreeNode;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * The LVT (Large Volume Tools) Data Import first converts the input data into an Octree using OctreeCreator
 * and then runs KTXCreator to create the KTX tiles for Horta.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("lvDataImport")
public class LVDataImport extends AbstractServiceProcessor<File> {

    static class LVDataImportArgs extends LVArgs {
        @Parameter(names = "-voxelSize", description = "Voxel size (in 'x,y,z' format)", required = true)
        String voxelSize;
    }

    private final WrappedServiceProcessor<OctreeCreator, List<File>> octreeCreator;
    private final WrappedServiceProcessor<KTXCreator,List<File>> ktxCreator;

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
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractAnyServiceResultHandler<File>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return true;
            }

            @Override
            public File collectResult(JacsServiceResult<?> depResults) {
                throw new UnsupportedOperationException();
            }

            @Override
            public File getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SampleProcessorResult>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<File>> process(JacsServiceData jacsServiceData) {
        LVDataImportArgs args = getArgs(jacsServiceData);

        final String inputTiffDir = args.inputDir;
        final String octreeDir = args.outputDir;
        final String ktxDir = StringUtils.appendIfMissing(octreeDir, "/") + "ktx";
        final int levels = args.levels;
        final String voxelSize = args.voxelSize;

        return octreeCreator.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                    .description("Create octree")
                    .build(),
                new ServiceArg("-inputDir", inputTiffDir),
                new ServiceArg("-outputDir", octreeDir),
                new ServiceArg("-levels", levels),
                new ServiceArg("-voxelSize", voxelSize))
            .thenCompose((JacsServiceResult<List<File>> octreeResult) ->
                ktxCreator.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                    .description("Create ktx tiles")
                        .waitFor(octreeResult.getJacsServiceData())
                    .build(),
                new ServiceArg("-inputDir", octreeDir),
                new ServiceArg("-outputDir", ktxDir),
                new ServiceArg("-levels", levels)))
            .thenApply((JacsServiceResult<List<File>> fileResult) ->
                new JacsServiceResult<>(jacsServiceData, new File(octreeDir)))
            ;
    }


    private LVDataImportArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LVDataImportArgs());
    }
}

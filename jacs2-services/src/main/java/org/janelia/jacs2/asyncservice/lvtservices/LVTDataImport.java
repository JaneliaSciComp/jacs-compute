package org.janelia.jacs2.asyncservice.lvtservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.sampleprocessing.SampleProcessorResult;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.domain.DomainConstants;
import org.janelia.model.domain.Reference;
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
@Named("lvtDataImport")
public class LVTDataImport extends AbstractServiceProcessor<File> {

    static class LVTDataImportArgs extends ServiceArgs {
        @Parameter(names = "-input", description = "Input directory containing TIFF files", required = true)
        String input;
        @Parameter(names = "-output", description = "Output directory for octree and ktx tiles", required = true)
        String output;
        @Parameter(names = "-levels", description = "Number of octree levels", required = false)
        Integer levels = 3;
        @Parameter(names = "-voxelSize", description = "Voxel size (in 'x,y,z' format)", required = true)
        String voxelSize;
        @Parameter(names = "-sampleName", description = "Name of sample in the Workstation. If null, no sample will be created.", required = false)
        String sampleName;
    }

    private final WrappedServiceProcessor<OctreeCreator, List<File>> octreeCreator;
    private final WrappedServiceProcessor<KTXCreator,List<File>> ktxCreator;

    @Inject
    private LegacyDomainDao dao;

    @Inject
    LVTDataImport(ServiceComputationFactory computationFactory,
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
        return ServiceArgs.getMetadata(LVTDataImport.class, new LVTDataImportArgs());
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
        LVTDataImportArgs args = getArgs(jacsServiceData);

        final String inputTiffDir = args.input;
        final String octreeDir = args.output;
        final String ktxDir = args.output+"/ktx";
        final int levels = args.levels;
        final String voxelSize = args.voxelSize;
        final String sampleName = args.sampleName;

        return octreeCreator.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                    .description("Create octree")
                    .build(),
                new ServiceArg("-input", inputTiffDir),
                new ServiceArg("-output", octreeDir),
                new ServiceArg("-levels", levels),
                new ServiceArg("-voxelSize", voxelSize))
            .thenCompose((JacsServiceResult<List<File>> fileResult) ->
                ktxCreator.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                    .description("Create ktx tiles")
                    .build(),
                new ServiceArg("-input", octreeDir),
                new ServiceArg("-output", ktxDir),
                new ServiceArg("-levels", levels)))
            .thenApply((JacsServiceResult<List<File>> fileResult) ->
                new JacsServiceResult<>(jacsServiceData, new File(octreeDir)))
            .thenApply((JacsServiceResult<File> result) -> {
                if (sampleName != null) {
                    try {
                        createTmSample(jacsServiceData.getOwner(), result.getResult().getAbsolutePath(), sampleName);
                    }
                    catch (Exception e) {
                        throw new UncheckedExecutionException("Error creating sample", e);
                    }
                }
                return result;
            });
    }

    // This is copy and pasted from JACSv1's TiledMicroscopeDAO. When that DAO gets ported over, it should be used instead.
    public TmSample createTmSample(String subjectKey, String filepath, String sampleName) throws Exception {
        logger.debug("createTmSample({}, {})",subjectKey,sampleName);
        TmSample sample = new TmSample();
        sample.setFilepath(filepath);
        sample.setName(sampleName);
        sample = dao.save(subjectKey, sample);
        TreeNode folder = dao.getOrCreateDefaultFolder(subjectKey, DomainConstants.NAME_TM_SAMPLE_FOLDER);
        dao.addChildren(subjectKey, folder, Arrays.asList(Reference.createFor(sample)));
        return sample;
    }

    private LVTDataImportArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LVTDataImportArgs());
    }
}

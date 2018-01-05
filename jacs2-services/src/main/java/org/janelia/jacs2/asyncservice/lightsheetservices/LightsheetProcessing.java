package org.janelia.jacs2.asyncservice.lightsheetservices;

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
 * This is copied from LVTDataImport.java in lvtservices
 * The LVT (Large Volume Tools) Data Import first converts the input data into an Octree using OctreeCreator
 * and then runs KTXCreator to create the KTX tiles for Horta.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
@Named("lightsheetProcessing")
public class LightsheetProcessing extends AbstractServiceProcessor<File> {

    static class LightsheetProcessingArgs extends ServiceArgs {
        @Parameter(names = "-input", description = "Input directory containing JSON files", required = true)
        String input;
    }

    private final WrappedServiceProcessor<DavidTestCreator,List<File>> davidTestCreator;

    @Inject
    private LegacyDomainDao dao;

    @Inject
    LightsheetProcessing(ServiceComputationFactory computationFactory,
                            JacsServiceDataPersistence jacsServiceDataPersistence,
                            @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                            DavidTestCreator davidTestCreator,
                            Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.davidTestCreator = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, davidTestCreator);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LightsheetProcessing.class, new LightsheetProcessingArgs());
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
        LightsheetProcessingArgs args = getArgs(jacsServiceData);

        final String inputJsonDir = args.input;

        return davidTestCreator.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                    .description("Run davidTestCreator")
                    .build(),
                new ServiceArg("-input", inputJsonDir));
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

    private LightsheetProcessingArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LightsheetProcessingArgs());
    }
}

package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.fasterxml.jackson.core.type.TypeReference;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.sample.AnatomicalArea;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.DefaultServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.fileservices.FileCopyProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

@Named("getSampleImageFiles")
public class GetSampleImageFilesProcessor extends AbstractBasicLifeCycleServiceProcessor<List<GetSampleImageFilesProcessor.GetSampleImageIntermediateData>, List<SampleImageFile>> {

    static class GetSampleImageIntermediateData {
        private final SampleImageFile sampleImageFile;

        GetSampleImageIntermediateData(SampleImageFile sampleImageFile) {
            this.sampleImageFile = sampleImageFile;
        }
    }

    private final SampleDataService sampleDataService;
    private final FileCopyProcessor fileCopyProcessor;

    @Inject
    GetSampleImageFilesProcessor(ServiceComputationFactory computationFactory,
                                 JacsServiceDataPersistence jacsServiceDataPersistence,
                                 @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                 SampleDataService sampleDataService,
                                 FileCopyProcessor fileCopyProcessor,
                                 Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.fileCopyProcessor = fileCopyProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(GetSampleImageFilesProcessor.class, new SampleServiceArgs());
    }

    @Override
    public ServiceResultHandler<List<SampleImageFile>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<SampleImageFile>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @SuppressWarnings("unchecked")
            @Override
            public List<SampleImageFile> collectResult(JacsServiceResult<?> depResults) {
                List<GetSampleImageIntermediateData> getSampleServiceData = (List<GetSampleImageIntermediateData>) depResults.getResult();
                return getSampleServiceData.stream()
                        .map(sd -> sd.sampleImageFile)
                        .collect(Collectors.toList());
            }

            public List<SampleImageFile> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SampleImageFile>>() {});
            }
        };
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        try {
            SampleServiceArgs args = getArgs(jacsServiceData);
            Path destinationDirectory = Paths.get(args.sampleDataRootDir, args.sampleLsmsSubDir);
            Files.createDirectories(destinationDirectory);
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected JacsServiceResult<List<GetSampleImageIntermediateData>> submitServiceDependencies(JacsServiceData jacsServiceData) {
        SampleServiceArgs args = getArgs(jacsServiceData);
        List<AnatomicalArea> anatomicalAreas =
                sampleDataService.getAnatomicalAreasBySampleIdObjectiveAndArea(jacsServiceData.getOwner(), args.sampleId, args.sampleObjective, args.sampleArea);
        // invoke child file copy services for all LSM files
        List<GetSampleImageIntermediateData> getSampleServiceData = anatomicalAreas.stream()
                .flatMap(ar -> ar.getTileLsmPairs()
                        .stream()
                        .flatMap(lsmp -> lsmp.getLsmFiles().stream())
                        .map(lsmf -> {
                            SampleImageFile sif = new SampleImageFile();
                            sif.setSampleId(args.sampleId);
                            sif.setId(lsmf.getId());
                            sif.setArchiveFilePath(lsmf.getFilepath());
                            sif.setWorkingFilePath(SampleServicesUtils.getImageFile(args.sampleDataRootDir, args.sampleLsmsSubDir, ar.getObjective(), ar.getName(), lsmf).toString());
                            sif.setArea(ar.getName());
                            sif.setChanSpec(lsmf.getChanSpec());
                            sif.setColorSpec(lsmf.getChannelColors());
                            sif.setObjective(ar.getObjective());
                            sif.setMetadataFilePath(lsmf.getFileName(FileType.LsmMetadata));
                            return sif;
                        }))
                .map(sif -> {
                    File imageWorkingFile = new File(sif.getWorkingFilePath());
                    if (!imageWorkingFile.exists()) {
                        JacsServiceData fileCopyService = fileCopyProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData).build(),
                                new ServiceArg("-src", sif.getArchiveFilePath()),
                                new ServiceArg("-dst", sif.getWorkingFilePath()));
                        submitDependencyIfNotFound(fileCopyService);
                    }
                    return new GetSampleImageIntermediateData(sif);
                })
                .collect(Collectors.toList());
        return new JacsServiceResult<>(jacsServiceData, getSampleServiceData);
    }

    @Override
    protected ServiceComputation<JacsServiceResult<List<GetSampleImageIntermediateData>>> processing(JacsServiceResult<List<GetSampleImageIntermediateData>> depResults) {
        return computationFactory.newCompletedComputation(depResults);
    }

    private SampleServiceArgs getArgs(JacsServiceData jacsServiceData) {
        return SampleServiceArgs.parse(jacsServiceData.getArgsArray(), new SampleServiceArgs());
    }

}

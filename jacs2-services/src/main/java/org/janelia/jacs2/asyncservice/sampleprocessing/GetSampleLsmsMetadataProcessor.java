package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.fasterxml.jackson.core.type.TypeReference;
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
import org.janelia.jacs2.asyncservice.lsmfileservices.LsmFileMetadataProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.util.List;

@Named("getSampleLsmMetadata")
public class GetSampleLsmsMetadataProcessor extends AbstractBasicLifeCycleServiceProcessor<GetSampleLsmsIntermediateResult, List<SampleImageFile>> {

    private final GetSampleImageFilesProcessor getSampleImageFilesProcessor;
    private final LsmFileMetadataProcessor lsmFileMetadataProcessor;

    @Inject
    GetSampleLsmsMetadataProcessor(ServiceComputationFactory computationFactory,
                                   JacsServiceDataPersistence jacsServiceDataPersistence,
                                   @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                   GetSampleImageFilesProcessor getSampleImageFilesProcessor,
                                   LsmFileMetadataProcessor lsmFileMetadataProcessor,
                                   Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.getSampleImageFilesProcessor = getSampleImageFilesProcessor;
        this.lsmFileMetadataProcessor = lsmFileMetadataProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(GetSampleLsmsMetadataProcessor.class, new SampleServiceArgs());
    }

    @Override
    public ServiceResultHandler<List<SampleImageFile>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<SampleImageFile>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public List<SampleImageFile> collectResult(JacsServiceResult<?> depResults) {
                GetSampleLsmsIntermediateResult result = (GetSampleLsmsIntermediateResult) depResults.getResult();
                return result.sampleImageFiles;
            }

            public List<SampleImageFile> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SampleImageFile>>() {});
            }
        };
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return new DefaultServiceErrorChecker(logger);
    }

    @Override
    protected JacsServiceResult<GetSampleLsmsIntermediateResult> submitServiceDependencies(JacsServiceData jacsServiceData) {
        SampleServiceArgs args = getArgs(jacsServiceData);

        JacsServiceData getSampleLsmsServiceRef = getSampleImageFilesProcessor.createServiceData(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .build(),
                new ServiceArg("-sampleId", args.sampleId.toString()),
                new ServiceArg("-objective", args.sampleObjective),
                new ServiceArg("-area", args.sampleArea),
                new ServiceArg("-sampleDataRootDir", args.sampleDataRootDir),
                new ServiceArg("-sampleLsmsSubDir", args.sampleLsmsSubDir)
        );
        JacsServiceData getSampleLsmsService = submitDependencyIfNotFound(getSampleLsmsServiceRef);
        return new JacsServiceResult<>(jacsServiceData, new GetSampleLsmsIntermediateResult(getSampleLsmsService.getId()));
    }

    @Override
    protected ServiceComputation<JacsServiceResult<GetSampleLsmsIntermediateResult>> processing(JacsServiceResult<GetSampleLsmsIntermediateResult> depResults) {
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    SampleServiceArgs args = getArgs(pd.getJacsServiceData());
                    JacsServiceData getSampleLsmsService = jacsServiceDataPersistence.findById(depResults.getResult().getChildServiceId());
                    List<SampleImageFile> sampleImageFiles = getSampleImageFilesProcessor.getResultHandler().getServiceDataResult(getSampleLsmsService);
                    sampleImageFiles.stream()
                            .forEach(sif -> {
                                File lsmImageFile = new File(sif.getWorkingFilePath());
                                File lsmMetadataFile = SampleServicesUtils.getImageMetadataFile(args.sampleDataRootDir, args.sampleSummarySubDir, sif.getObjective(), sif.getArea(), lsmImageFile).toFile();
                                if (!lsmMetadataFile.exists()) {
                                    JacsServiceData lsmMetadataService = lsmFileMetadataProcessor.createServiceData(new ServiceExecutionContext.Builder(depResults.getJacsServiceData())
                                                    .waitFor(getSampleLsmsService)
                                                    .build(),
                                            new ServiceArg("-inputLSM", lsmImageFile.getAbsolutePath()),
                                            new ServiceArg("-outputLSMMetadata", lsmMetadataFile.getAbsolutePath())
                                    );
                                    submitDependencyIfNotFound(lsmMetadataService);
                                }
                                sif.setMetadataFilePath(lsmMetadataFile.getAbsolutePath());
                                depResults.getResult().addSampleImageFile(sif);
                            });
                    return pd;
                });
    }

    private SampleServiceArgs getArgs(JacsServiceData jacsServiceData) {
        return SampleServiceArgs.parse(jacsServiceData.getArgsArray(), new SampleServiceArgs());
    }

}

package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.fasterxml.jackson.core.type.TypeReference;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.lsmfileservices.LsmFileMetadataProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.model.service.RegisteredJacsNotification;
import org.janelia.model.service.JacsServiceData;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

@Named("getSampleLsmMetadata")
public class GetSampleLsmsMetadataProcessor extends AbstractServiceProcessor<List<SampleImageFile>> {

    private final WrappedServiceProcessor<GetSampleImageFilesProcessor, List<SampleImageFile>> getSampleImageFilesProcessor;
    private final WrappedServiceProcessor<LsmFileMetadataProcessor, File> lsmFileMetadataProcessor;

    @Inject
    GetSampleLsmsMetadataProcessor(ServiceComputationFactory computationFactory,
                                   JacsServiceDataPersistence jacsServiceDataPersistence,
                                   @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                   GetSampleImageFilesProcessor getSampleImageFilesProcessor,
                                   LsmFileMetadataProcessor lsmFileMetadataProcessor,
                                   Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.getSampleImageFilesProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, getSampleImageFilesProcessor);
        this.lsmFileMetadataProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, lsmFileMetadataProcessor);
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
                JacsServiceResult<List<SampleImageFile>> result = (JacsServiceResult<List<SampleImageFile>>)depResults;
                return result.getResult();
            }

            public List<SampleImageFile> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SampleImageFile>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<SampleImageFile>>> process(JacsServiceData jacsServiceData) {
        SampleServiceArgs args = getArgs(jacsServiceData);
        return getSampleImageFilesProcessor.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .build(),
                new ServiceArg("-sampleId", args.sampleId.toString()),
                new ServiceArg("-objective", args.sampleObjective),
                new ServiceArg("-area", args.sampleArea),
                new ServiceArg("-sampleDataRootDir", args.sampleDataRootDir),
                new ServiceArg("-sampleLsmsSubDir", args.sampleLsmsSubDir)
        ).thenCompose((JacsServiceResult<List<SampleImageFile>> sifr) -> {
            List<SampleImageFile> sampleImageFiles = sifr.getResult();
            List<ServiceComputation<?>> lsmMetadataComputations = sampleImageFiles.stream()
                    .map((SampleImageFile sif) -> {
                        File lsmImageFile = new File(sif.getWorkingFilePath());
                        File lsmMetadataFile = SampleServicesUtils.getImageMetadataFile(args.sampleDataRootDir, args.sampleSummarySubDir, sif.getObjective(), sif.getArea(), lsmImageFile).toFile();
                        if (!lsmMetadataFile.exists()) {
                            return lsmFileMetadataProcessor.process(
                                    new ServiceExecutionContext.Builder(jacsServiceData)
                                            .waitFor(sifr.getJacsServiceData())
                                            .registerProcessingNotification(
                                                    FlylightSampleEvents.LSM_METADATA,
                                                    jacsServiceData.getProcessingStageNotification(FlylightSampleEvents.LSM_METADATA, new RegisteredJacsNotification())
                                                            .map(n -> n.addNotificationField("sampleId", sif.getSampleId())
                                                                            .addNotificationField("lsmId", sif.getId())
                                                                            .addNotificationField("objective", sif.getObjective())
                                                                            .addNotificationField("area", sif.getArea())
                                                            )
                                            )
                                            .build(),
                                    new ServiceArg("-inputLSM", lsmImageFile.getAbsolutePath()),
                                    new ServiceArg("-outputLSMMetadata", lsmMetadataFile.getAbsolutePath())
                            ).thenApply(fileJacsServiceResult -> {
                                sif.setMetadataFilePath(fileJacsServiceResult.getResult().getAbsolutePath());
                                return sif;
                            });
                        } else {
                            sif.setMetadataFilePath(lsmMetadataFile.getAbsolutePath());
                            return computationFactory.newCompletedComputation(sif);
                        }
                    })
                    .collect(Collectors.toList());
            return computationFactory
                    .newCompletedComputation(sampleImageFiles)
                    .thenCombineAll(lsmMetadataComputations, (List<SampleImageFile> sifs, List<?> lsmsWithMetadataResults) -> (List<SampleImageFile>) lsmsWithMetadataResults);
        }).thenSuspendUntil(this.suspendCondition(jacsServiceData)
        ).thenApply((ContinuationCond.Cond<List<SampleImageFile>> lsmsWithMetadataCond) -> this.updateServiceResult(jacsServiceData, lsmsWithMetadataCond.getState()));
    }

    private SampleServiceArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SampleServiceArgs());
    }

}

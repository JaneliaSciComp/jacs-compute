package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.tools.LSMProcessingTools;
import org.janelia.jacs2.asyncservice.sampleprocessing.zeiss.LSMDetectionChannel;
import org.janelia.jacs2.asyncservice.sampleprocessing.zeiss.LSMMetadata;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Named("updateLSMMetadata")
public class UpdateLSMsMetadataProcessor extends AbstractBasicLifeCycleServiceProcessor<Number, Void> {

    private final SampleDataService sampleDataService;
    private final GetSampleLsmsMetadataProcessor getSampleLsmsMetadataProcessor;

    @Inject
    UpdateLSMsMetadataProcessor(ServiceComputationFactory computationFactory,
                                JacsServiceDataPersistence jacsServiceDataPersistence,
                                @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                SampleDataService sampleDataService,
                                GetSampleLsmsMetadataProcessor getSampleLsmsMetadataProcessor,
                                Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.getSampleLsmsMetadataProcessor = getSampleLsmsMetadataProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(this.getClass(), new SampleServiceArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }
        };
    }

    @Override
    protected JacsServiceResult<Number> submitServiceDependencies(JacsServiceData jacsServiceData) {
        SampleServiceArgs args = getArgs(jacsServiceData);
        JacsServiceData getSampleLsmMetadataServiceRef = getSampleLsmsMetadataProcessor.createServiceData(new ServiceExecutionContext(jacsServiceData),
                new ServiceArg("-sampleId", args.sampleId.toString()),
                new ServiceArg("-objective", args.sampleObjective),
                new ServiceArg("-sampleDataDir", args.sampleDataDir)
        );
        JacsServiceData getSampleLsmMetadataService = submitDependencyIfNotPresent(jacsServiceData, getSampleLsmMetadataServiceRef);
        return new JacsServiceResult<>(jacsServiceData, getSampleLsmMetadataService.getId());
    }

    @Override
    protected ServiceComputation<JacsServiceResult<Number>> processing(JacsServiceResult<Number> depResults) {
        return computationFactory.newCompletedComputation(depResults)
            .thenApply(pd -> {
                JacsServiceData getSampleLsmsMetadatService = jacsServiceDataPersistence.findById(depResults.getResult());
                List<SampleImageFile> sampleImageFiles = getSampleLsmsMetadataProcessor.getResultHandler().getServiceDataResult(getSampleLsmsMetadatService);
                sampleImageFiles.forEach(simdf -> {
                    LSMMetadata lsmMetadata = LSMProcessingTools.getLSMMetadata(simdf.getMetadataFilePath());
                    List<String> colors = new ArrayList<>();
                    List<String> dyeNames = new ArrayList<>();
                    if (CollectionUtils.isNotEmpty(lsmMetadata.getChannels())) {
                        lsmMetadata.getChannels().forEach(channel -> {
                            colors.add(channel.getColor());
                            LSMDetectionChannel detection = lsmMetadata.getDetectionChannel(channel);
                            if (detection != null) {
                                dyeNames.add(detection.getDyeName());
                            } else {
                                dyeNames.add("Unknown");
                            }
                        });
                    }
                    LSMImage lsmImage = sampleDataService.getLSMsByIds(pd.getJacsServiceData().getOwner(), ImmutableList.of(simdf.getId())).stream().findFirst().orElse(null);
                    if (lsmImage == null) {
                        logger.warn("No LSM IMAGE found for sample {} with id = {}", simdf.getSampleId(), simdf.getId());
                    } else {
                        boolean lsmUpdated = false;
                        if (CollectionUtils.isNotEmpty(colors)) {
                            lsmImage.setChannelColors(Joiner.on(',').join(colors));
                            lsmUpdated = true;
                        }
                        if (CollectionUtils.isNotEmpty(dyeNames)) {
                            lsmImage.setChannelDyeNames(Joiner.on(',').join(dyeNames));
                            lsmUpdated = true;
                        }
                        if (lsmUpdated) {
                            sampleDataService.updateLSM(lsmImage);
                        }
                        sampleDataService.updateLSMMetadataFile(lsmImage, simdf.getMetadataFilePath());
                    }
                });
                return pd;
            });
    }

    private SampleServiceArgs getArgs(JacsServiceData jacsServiceData) {
        return SampleServiceArgs.parse(jacsServiceData.getArgsArray(), new SampleServiceArgs());
    }

}

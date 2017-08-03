package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.it.jacs.model.domain.enums.FileType;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.tools.LSMProcessingTools;
import org.janelia.jacs2.asyncservice.sampleprocessing.zeiss.LSMDetectionChannel;
import org.janelia.jacs2.asyncservice.sampleprocessing.zeiss.LSMMetadata;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.model.DomainModelUtils;
import org.janelia.jacs2.model.EntityFieldValueHandler;
import org.janelia.jacs2.model.SetFieldValueHandler;
import org.janelia.jacs2.model.jacsservice.RegisteredJacsNotification;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Named("updateSampleLSMMetadata")
public class UpdateSampleLSMMetadataProcessor extends AbstractBasicLifeCycleServiceProcessor<List<SampleImageFile>, GetSampleLsmsIntermediateResult> {

    static class UpdateLSMsMetadataArgs extends SampleServiceArgs {
        @Parameter(names = "-channelDyeSpec", description = "Channel dye spec", required = false)
        String channelDyeSpec;
        @Parameter(names = "-overwrite", description = "Overwrite the metadata if it exists, otherwise the LSM metadata will not be overwritten", required = false)
        boolean overwrite;
    }

    private final SampleDataService sampleDataService;
    private final GetSampleLsmsMetadataProcessor getSampleLsmsMetadataProcessor;

    @Inject
    UpdateSampleLSMMetadataProcessor(ServiceComputationFactory computationFactory,
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
        return ServiceArgs.getMetadata(UpdateSampleLSMMetadataProcessor.class, new SampleServiceArgs());
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
    protected JacsServiceResult<GetSampleLsmsIntermediateResult> submitServiceDependencies(JacsServiceData jacsServiceData) {
        SampleServiceArgs args = getArgs(jacsServiceData);
        JacsServiceData getSampleLsmMetadataServiceRef = getSampleLsmsMetadataProcessor.createServiceData(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .registerProcessingStageNotification(
                                FlylightSampleEvents.LSM_METADATA,
                                jacsServiceData.getProcessingStageNotification(FlylightSampleEvents.LSM_METADATA, new RegisteredJacsNotification())
                                        .map(n -> n.addNotificationField("sampleId", args.sampleId)
                                                        .addNotificationField("objective", args.sampleObjective)
                                                        .addNotificationField("area", args.sampleArea)
                                        )
                        )
                        .build(),
                new ServiceArg("-sampleId", args.sampleId),
                new ServiceArg("-objective", args.sampleObjective),
                new ServiceArg("-area", args.sampleArea),
                new ServiceArg("-sampleDataRootDir", args.sampleDataRootDir),
                new ServiceArg("-sampleLsmsSubDir", args.sampleLsmsSubDir),
                new ServiceArg("-sampleSummarySubDir", args.sampleSummarySubDir)
        );
        JacsServiceData getSampleLsmMetadataService = submitDependencyIfNotFound(getSampleLsmMetadataServiceRef);
        return new JacsServiceResult<>(jacsServiceData, new GetSampleLsmsIntermediateResult(getSampleLsmMetadataService.getId()));
    }

    @Override
    protected ServiceComputation<JacsServiceResult<GetSampleLsmsIntermediateResult>> processing(JacsServiceResult<GetSampleLsmsIntermediateResult> depResults) {
        UpdateLSMsMetadataArgs args = getArgs(depResults.getJacsServiceData());
        return computationFactory.newCompletedComputation(depResults)
            .thenApply(pd -> {
                JacsServiceData getSampleLsmsMetadataService = jacsServiceDataPersistence.findById(pd.getResult().getChildServiceId());
                List<SampleImageFile> sampleImageFiles = getSampleLsmsMetadataProcessor.getResultHandler().getServiceDataResult(getSampleLsmsMetadataService);
                sampleImageFiles.forEach(sif -> {
                    LSMImage lsmImage = sampleDataService.getLSMsByIds(pd.getJacsServiceData().getOwner(), ImmutableList.of(sif.getId())).stream().findFirst().orElse(null);
                    if (lsmImage == null) {
                        throw new IllegalStateException("No LSM found for " + sif.getSampleId() + ":" + sif.getId());
                    }
                    if (!lsmImage.hasFileName(FileType.LsmMetadata) || args.overwrite || FileUtils.fileNotExists(lsmImage.getFileName(FileType.LsmMetadata))) {
                        updateLSM(lsmImage, sif.getMetadataFilePath(), args.channelDyeSpec);
                    }
                    pd.getResult().addSampleImageFile(sif);
                });
                return pd;
            });
    }

    private UpdateLSMsMetadataArgs getArgs(JacsServiceData jacsServiceData) {
        return SampleServiceArgs.parse(jacsServiceData.getArgsArray(), new UpdateLSMsMetadataArgs());
    }

    private void updateLSM(LSMImage lsm, String lsmMetadataFilePath, String channelDyeSpec) {
        logger.debug("Update LSM metadata for {} to {}", lsm, lsmMetadataFilePath);
        LSMMetadata lsmMetadata = LSMProcessingTools.getLSMMetadata(lsmMetadataFilePath);
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
        Map<String, EntityFieldValueHandler<?>> updatedLsmFields = new LinkedHashMap<>();
        if (CollectionUtils.isNotEmpty(colors)) {
            lsm.setChannelColors(Joiner.on(',').join(colors));
            updatedLsmFields.put("channelColors", new SetFieldValueHandler<>(lsm.getChannelColors()));
        }
        if (CollectionUtils.isNotEmpty(dyeNames)) {
            lsm.setChannelDyeNames(Joiner.on(',').join(dyeNames));
            updatedLsmFields.put("channelDyeNames", new SetFieldValueHandler<>(lsm.getChannelDyeNames()));
        }
        if (StringUtils.isBlank(lsm.getChanSpec())) {
            updatedLsmFields.putAll(updateChanSpec(lsm, channelDyeSpec));
        }
        updatedLsmFields.putAll(DomainModelUtils.setFullPathForFileType(lsm, FileType.LsmMetadata, lsmMetadataFilePath));
        sampleDataService.updateLSM(lsm, updatedLsmFields);
   }

    private Map<String, EntityFieldValueHandler<?>> updateChanSpec(LSMImage lsm, String channelDyeSpec) {
        Map<String, EntityFieldValueHandler<?>> updatedLsmFields = new LinkedHashMap<>();
        Set<String> referenceDyes = new LinkedHashSet<>();
        if (StringUtils.isNotBlank(channelDyeSpec)) {
            Pair<Multimap<String, String>, Map<String, String>> channelDyesMapData = LSMProcessingTools.parseChannelDyeSpec(channelDyeSpec);
            if (!channelDyesMapData.getLeft().containsKey("reference")) {
                logger.warn("No reference dye defined in {}", channelDyeSpec);
            } else {
                referenceDyes.addAll(channelDyesMapData.getLeft().get("reference"));
            }
        }
        if (referenceDyes.isEmpty() || StringUtils.isBlank(lsm.getChannelDyeNames())) {
            int numChannels;
            if (StringUtils.isBlank(lsm.getChannelDyeNames())) {
                if (lsm.getNumChannels() == null) {
                    throw new IllegalStateException("No channel dye names and no num channels available for " + lsm + " in order to generate the channel spec");
                } else {
                    numChannels = lsm.getNumChannels();
                }
            } else {
                List<String> channelDyeNames = LSMProcessingTools.parseChannelComponents(lsm.getChannelDyeNames());
                numChannels = channelDyeNames.size();
            }
            // For legacy LSMs without chanspec or dyespec, we assume that the reference is the first channel and the rest are signal
            lsm.setChanSpec(LSMProcessingTools.createChanSpec(numChannels, 1));
            updatedLsmFields.put("chanSpec", new SetFieldValueHandler<>(lsm.getChanSpec()));
        } else {
            List<String> channelDyeNames = LSMProcessingTools.parseChannelComponents(lsm.getChannelDyeNames());
            lsm.setChanSpec(LSMProcessingTools.createChanSpec(channelDyeNames, referenceDyes));
            updatedLsmFields.put("chanSpec", new SetFieldValueHandler<>(lsm.getChanSpec()));
        }
        return updatedLsmFields;
    }
}

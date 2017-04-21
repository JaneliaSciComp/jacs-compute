package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Named("updateSampleLSMMetadata")
public class UpdateSampleLSMMetadataProcessor extends AbstractBasicLifeCycleServiceProcessor<GetSampleLsmsIntermediateResult, List<SampleImageFile>> {

    static class UpdateLSMsMetadataArgs extends SampleServiceArgs {
        @Parameter(names = "-channelDyeSpec", description = "Channel dye spec", required = false)
        String channelDyeSpec;
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
        return ServiceArgs.getMetadata(this.getClass(), new SampleServiceArgs());
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
                return ServiceDataUtils.stringToAny(jacsServiceData.getStringifiedResult(), new TypeReference<List<SampleImageFile>>() {});
            }
        };
    }

    @Override
    protected JacsServiceResult<GetSampleLsmsIntermediateResult> submitServiceDependencies(JacsServiceData jacsServiceData) {
        SampleServiceArgs args = getArgs(jacsServiceData);
        JacsServiceData getSampleLsmMetadataServiceRef = getSampleLsmsMetadataProcessor.createServiceData(new ServiceExecutionContext(jacsServiceData),
                new ServiceArg("-sampleId", args.sampleId.toString()),
                new ServiceArg("-objective", args.sampleObjective),
                new ServiceArg("-area", args.sampleArea),
                new ServiceArg("-sampleDataDir", args.sampleDataDir)
        );
        JacsServiceData getSampleLsmMetadataService = submitDependencyIfNotPresent(jacsServiceData, getSampleLsmMetadataServiceRef);
        return new JacsServiceResult<>(jacsServiceData, new GetSampleLsmsIntermediateResult(getSampleLsmMetadataService.getId()));
    }

    @Override
    protected ServiceComputation<JacsServiceResult<GetSampleLsmsIntermediateResult>> processing(JacsServiceResult<GetSampleLsmsIntermediateResult> depResults1) {
        UpdateLSMsMetadataArgs args = getArgs(depResults1.getJacsServiceData());
        return computationFactory.newCompletedComputation(depResults1)
            .thenApply(pd -> {
                JacsServiceData getSampleLsmsMetadataService = jacsServiceDataPersistence.findById(pd.getResult().getSampleLsmsServiceDataId);
                List<SampleImageFile> sampleImageFiles = getSampleLsmsMetadataProcessor.getResultHandler().getServiceDataResult(getSampleLsmsMetadataService);
                sampleImageFiles.forEach(sif -> {
                    LSMImage lsmImage = sampleDataService.getLSMsByIds(pd.getJacsServiceData().getOwner(), ImmutableList.of(sif.getId())).stream().findFirst().orElse(null);
                    if (lsmImage == null) {
                        throw new IllegalStateException("No LSM found for " + sif.getSampleId() + ":" + sif.getId());
                    }
                    updateLSM(lsmImage, sif.getMetadataFilePath(), args.channelDyeSpec);
                    pd.getResult().addSampleImageFile(sif);
                });
                return pd;
            });
    }

    private UpdateLSMsMetadataArgs getArgs(JacsServiceData jacsServiceData) {
        return SampleServiceArgs.parse(jacsServiceData.getArgsArray(), new UpdateLSMsMetadataArgs());
    }

    private void updateLSM(LSMImage lsm, String lsmMetadataFilePath, String channelDyeSpec) {
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
        boolean lsmUpdated = false;
        if (CollectionUtils.isNotEmpty(colors)) {
            lsm.setChannelColors(Joiner.on(',').join(colors));
            lsmUpdated = true;
        }
        if (CollectionUtils.isNotEmpty(dyeNames)) {
            lsm.setChannelDyeNames(Joiner.on(',').join(dyeNames));
            lsmUpdated = true;
        }
        if (StringUtils.isBlank(lsm.getChanSpec())) {
            updateChanSpec(lsm, channelDyeSpec);
            lsmUpdated = true;
        }
        if (lsmUpdated) {
            sampleDataService.updateLSM(lsm);
        }
        sampleDataService.updateLSMMetadataFile(lsm, lsmMetadataFilePath);
   }

    private void updateChanSpec(LSMImage lsm, String channelDyeSpec) {
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
                List<String> channelDyeNames = LSMProcessingTools.parseChannelDyes(lsm.getChannelDyeNames());
                numChannels = channelDyeNames.size();
            }
            // For legacy LSMs without chanspec or dyespec, we assume that the reference is the first channel and the rest are signal
            lsm.setChanSpec(LSMProcessingTools.createChanSpec(numChannels, 1));
        } else {
            List<String> channelDyeNames = LSMProcessingTools.parseChannelDyes(lsm.getChannelDyeNames());
            lsm.setChanSpec(LSMProcessingTools.createChanSpec(channelDyeNames, referenceDyes));
        }
    }
}
package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.janelia.it.jacs.model.domain.sample.LSMImage;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.GroupAndMontageFolderImagesProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Named("sampleLSMSummary")
public class SampleLSMSummaryProcessor extends AbstractBasicLifeCycleServiceProcessor<SampleLSMSummaryProcessor.SampleLSMSummaryIntermediateResult, List<LSMSummary>> {

    static class MontageParameters {
        final Number montageServiceId;
        final SampleImageMIPsFile montageData;

        public MontageParameters(Number montageServiceId, SampleImageMIPsFile montageData) {
            this.montageServiceId = montageServiceId;
            this.montageData = montageData;
        }
    }

    static class SampleLSMSummaryIntermediateResult extends GetSampleLsmsIntermediateResult {
        final Number mipMapsServiceDataId;
        final List<MontageParameters> montageCalls = new ArrayList<>();

        SampleLSMSummaryIntermediateResult(Number getSampleLsmsServiceDataId, Number mipMapsServiceDataId) {
            super(getSampleLsmsServiceDataId);
            this.mipMapsServiceDataId = mipMapsServiceDataId;
        }

        void addMontage(MontageParameters montageCall) {
            montageCalls.add(montageCall);
        }
    }

    static class SampleLSMSummaryArgs extends SampleServiceArgs {
        @Parameter(names = "-channelDyeSpec", description = "Channel dye spec", required = false)
        String channelDyeSpec;
        @Parameter(names = "-basicMipMapsOptions", description = "Basic MIPS and Movies Options", required = false)
        String basicMipMapsOptions = "mips:movies:legends:bcomp";
    }

    private final SampleDataService sampleDataService;
    private final UpdateSampleLSMMetadataProcessor updateSampleLSMMetadataProcessor;
    private final GetSampleMIPsAndMoviesProcessor getSampleMIPsAndMoviesProcessor;
    private final GroupAndMontageFolderImagesProcessor groupAndMontageFolderImagesProcessor;


    @Inject
    SampleLSMSummaryProcessor(ServiceComputationFactory computationFactory,
                              JacsServiceDataPersistence jacsServiceDataPersistence,
                              @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                              SampleDataService sampleDataService,
                              UpdateSampleLSMMetadataProcessor updateSampleLSMMetadataProcessor,
                              GetSampleMIPsAndMoviesProcessor getSampleMIPsAndMoviesProcessor,
                              GroupAndMontageFolderImagesProcessor groupAndMontageFolderImagesProcessor,
                              Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.updateSampleLSMMetadataProcessor = updateSampleLSMMetadataProcessor;
        this.getSampleMIPsAndMoviesProcessor = getSampleMIPsAndMoviesProcessor;
        this.groupAndMontageFolderImagesProcessor = groupAndMontageFolderImagesProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(this.getClass(), new SampleServiceArgs());
    }

    @Override
    public ServiceResultHandler<List<LSMSummary>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<LSMSummary>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public List<LSMSummary> collectResult(JacsServiceResult<?> depResults) {
                SampleLSMSummaryIntermediateResult result = (SampleLSMSummaryIntermediateResult) depResults.getResult();
                return result.montageCalls.stream().map(mc -> {
                    JacsServiceData montageServiceData = jacsServiceDataPersistence.findById(mc.montageServiceId);
                    LSMSummary lsmSummary = new LSMSummary();
                    lsmSummary.setSampleImageFile(mc.montageData.getSampleImageFile());
                    lsmSummary.setMipsResultsDir(mc.montageData.getMipsResultsDir());
                    lsmSummary.setMips(mc.montageData.getMips());
                    lsmSummary.setMontageResultsByType(groupAndMontageFolderImagesProcessor.getResultHandler().getServiceDataResult(montageServiceData));
                    return lsmSummary;
                })
                .collect(Collectors.toList());
            }

            @Override
            public List<LSMSummary> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.stringToAny(jacsServiceData.getStringifiedResult(), new TypeReference<List<LSMSummary>>() {});
            }
        };
    }

    @Override
    protected JacsServiceResult<SampleLSMSummaryIntermediateResult> submitServiceDependencies(JacsServiceData jacsServiceData) {
        SampleLSMSummaryArgs args = getArgs(jacsServiceData);
        // update samp[lpe
        JacsServiceData updateSampleLsmMetadataService = updateSampleLSMMetadataProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Update sample LSM metadata")
                        .build(),
                new ServiceArg("-sampleId", args.sampleId.toString()),
                new ServiceArg("-objective", args.sampleObjective),
                new ServiceArg("-area", args.sampleArea),
                new ServiceArg("-channelDyeSpec", args.channelDyeSpec),
                new ServiceArg("-sampleDataDir", args.sampleDataDir)
        );
        updateSampleLsmMetadataService = submitDependencyIfNotPresent(jacsServiceData, updateSampleLsmMetadataService);
        JacsServiceData getSampleMipMapsService = getSampleMIPsAndMoviesProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Generate MIPs and Movies for the sample")
                        .waitFor(updateSampleLsmMetadataService)
                        .build(),
                new ServiceArg("-sampleId", args.sampleId.toString()),
                new ServiceArg("-objective", args.sampleObjective),
                new ServiceArg("-area", args.sampleArea),
                new ServiceArg("-options", args.basicMipMapsOptions),
                new ServiceArg("-sampleDataDir", args.sampleDataDir)
        );
        getSampleMipMapsService = submitDependencyIfNotPresent(jacsServiceData, getSampleMipMapsService);

        return new JacsServiceResult<>(jacsServiceData, new SampleLSMSummaryIntermediateResult(updateSampleLsmMetadataService.getId(), getSampleMipMapsService.getId()));
    }

    @Override
    protected ServiceComputation<JacsServiceResult<SampleLSMSummaryIntermediateResult>> processing(JacsServiceResult<SampleLSMSummaryIntermediateResult> depResults) {
        JacsServiceData jacsServiceData = depResults.getJacsServiceData();
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    JacsServiceData mipMapsService = jacsServiceDataPersistence.findById(depResults.getResult().mipMapsServiceDataId);
                    List<SampleImageMIPsFile> sampleImageFiles = getSampleMIPsAndMoviesProcessor.getResultHandler().getServiceDataResult(mipMapsService);
                    sampleImageFiles.stream()
                            .forEach(sif -> {
                                // for each sample image file invoke basic mipmaps and montage service
                                JacsServiceData montageService = groupAndMontageFolderImagesProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                                                .description("Montage PNG images")
                                                .waitFor(mipMapsService)
                                                .build(),
                                        new ServiceArg("-input", sif.getMipsResultsDir()),
                                        new ServiceArg("-output", sif.getMipsResultsDir())
                                );
                                montageService = submitDependencyIfNotPresent(jacsServiceData, montageService);
                                MontageParameters montageCall = new MontageParameters(montageService.getId(), sif);
                                pd.getResult().addMontage(montageCall);
                            })
                            ;
                    return pd;
                })
                ;
    }

    @Override
    protected JacsServiceResult<List<LSMSummary>> updateServiceResult(JacsServiceResult<SampleLSMSummaryIntermediateResult> depsResult) {
        JacsServiceResult<List<LSMSummary>> result = super.updateServiceResult(depsResult);
        result.getResult().forEach(lsmSummary -> {
            Optional<LSMImage> lsmImage = sampleDataService.getLSMsByIds(result.getJacsServiceData().getOwner(), ImmutableList.of(lsmSummary.getSampleImageFile().getId())).stream().findFirst();
            if (!lsmImage.isPresent()) {
                throw new IllegalStateException("No LSM image found for " + lsmSummary.getSampleImageFile().getId());
            }
            updateLSM(lsmImage.get(), lsmSummary);
        });
        return result;
    }

    private SampleLSMSummaryArgs getArgs(JacsServiceData jacsServiceData) {
        return SampleServiceArgs.parse(jacsServiceData.getArgsArray(), new SampleLSMSummaryArgs());
    }

    private void updateLSM(LSMImage lsmImage, LSMSummary lsmSummary) {
        logger.info("Update LSM {} with {}", lsmImage, lsmSummary);
        // FIXME
    }
}

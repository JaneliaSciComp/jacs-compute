package org.janelia.jacs2.asyncservice.imageservices;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractFileListServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Named("deconvolution")
public class DeconvolutionProcessor extends AbstractServiceProcessor<List<File>> {

    private final WrappedServiceProcessor<DeconvolutionJobsProcessor, Void> deconvolutionJobsProcessor;
    private final DeconvolutionHelper deconvolutionHelper;

    @Inject
    DeconvolutionProcessor(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           DeconvolutionJobsProcessor deconvolutionJobsProcessor,
                           ObjectMapper objectMapper,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.deconvolutionJobsProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, deconvolutionJobsProcessor);
        this.deconvolutionHelper = new DeconvolutionHelper(objectMapper);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(DeconvolutionProcessor.class, new DeconvolutionArgs());
    }

    @Override
    public ServiceResultHandler<List<File>> getResultHandler() {
        return new AbstractFileListServiceResultHandler() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                DeconvolutionArgs args = getArgs(depResults.getJacsServiceData());
                return args.tileChannelConfigurationFiles.stream()
                        .map(channelInput -> getResultNameForChannel(channelInput))
                        .map(channelResult -> FileUtils.fileExists(channelResult))
                        .reduce((r1, r2) -> r1 && r2)
                        .orElse(false)
                        ;
            }

            @Override
            public List<File> collectResult(JacsServiceResult<?> depResults) {
                DeconvolutionArgs args = getArgs(depResults.getJacsServiceData());
                return args.tileChannelConfigurationFiles.stream()
                        .map(channelInput -> getResultNameForChannel(channelInput))
                        .map(channelResult -> Paths.get(channelResult).toFile())
                        .collect(Collectors.toList())
                        ;
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<File>>> process(JacsServiceData jacsServiceData) {
        DeconvolutionArgs args = getArgs(jacsServiceData);
        return deconvolutionJobsProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Run deconvolution for all image tiles")
                        .build(),
                new ServiceArg("-i", args.tileChannelConfigurationFiles.stream().reduce((c1, c2) -> c1 + "," + c2).orElse("")),
                new ServiceArg("-p", args.psfFiles.stream().reduce((f1, f2) -> f1 + "," + f2).orElse("")),
                new ServiceArg("-z", args.psfZStep),
                new ServiceArg("-n", args.nIterationsPerChannel.stream().map(Object::toString).reduce((f1, f2) -> f1 + "," + f2).orElse("")),
                new ServiceArg("-v", args.backgroundValue),
                new ServiceArg("-c", args.coresPerTask))
                .thenApply(r -> {
                    List<File> channelDeconvolutionResultFiles = args.tileChannelConfigurationFiles.stream()
                            .map(channelConfigFile -> {
                                List<Map<String, Object>> channelTileConfigs = deconvolutionHelper.loadJsonConfiguration(channelConfigFile,
                                        new TypeReference<List<Map<String, Object>>>() {}).orElseGet(() -> ImmutableList.of());
                                String deconvOutputDir = deconvolutionHelper.mapToDeconvOutputDir(channelConfigFile);
                                List<Map<String, Object>> channelDeconvConfigs = channelTileConfigs.stream()
                                        .map(tileConfig -> {
                                            String tileDeconvolutionFile = deconvolutionHelper.getTileDeconvFile(tileConfig, deconvOutputDir);
                                            tileConfig.put("file", tileDeconvolutionFile);
                                            return tileConfig;
                                        })
                                        .collect(Collectors.toList());

                                String channelDeconvResultFile = getResultNameForChannel(channelConfigFile);
                                deconvolutionHelper.saveJsonConfiguration(channelDeconvConfigs, channelDeconvResultFile);
                                return new File(channelDeconvResultFile);
                            })
                            .collect(Collectors.toList());
                    return updateServiceResult(jacsServiceData, channelDeconvolutionResultFiles);
                })
                .thenApply(r -> updateServiceResult(jacsServiceData, getResultHandler().collectResult(r)))
                ;
    }

    private DeconvolutionArgs getArgs(JacsServiceData jacsServiceData) {
        DeconvolutionArgs args = ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new DeconvolutionArgs());
        args.validate();
        return args;
    }

    private String getResultNameForChannel(String channelInputName) {
        return deconvolutionHelper.mapToResultName(channelInputName, null, "-decon");
    }

}

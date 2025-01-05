package org.janelia.jacs2.asyncservice.imageservices;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.DelegateServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Dependent
@Named("vaa3dStitch")
public class Vaa3dStitchProcessor extends AbstractServiceProcessor<File> {

    private static final String STITCHED_IMAGE_INFO_FILENAME = "stitched_image.tc";

    static class Vaa3dStitchArgs extends ServiceArgs {
        @Parameter(names = "-inputDir", description = "Input directory", required = true)
        String inputDir;
        @Parameter(names = "-refchannel", description = "Reference channel")
        int referenceChannel = 4;
        @Parameter(names = {"-p", "-pluginParams"}, description = "Other plugin parameters")
        List<String> pluginParams = new ArrayList<>();
    }

    private final DelegateServiceProcessor<Vaa3dPluginProcessor, List<File>> vaa3dPluginProcessor;

    @Inject
    Vaa3dStitchProcessor(ServiceComputationFactory computationFactory,
                         JacsServiceDataPersistence jacsServiceDataPersistence,
                         @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                         Vaa3dPluginProcessor vaa3dPluginProcessor,
                         Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.vaa3dPluginProcessor = new DelegateServiceProcessor<>(vaa3dPluginProcessor, jacsServiceData -> {
            Vaa3dStitchArgs args = getArgs(jacsServiceData);
            return ImmutableList.of(
                    new ServiceArg("-plugin", "imageStitch.so"),
                    new ServiceArg("-pluginFunc", "v3dstitch"),
                    new ServiceArg("-input", args.inputDir),
                    new ServiceArg("-pluginParams", String.format("#c %d", args.referenceChannel)),
                    new ServiceArg("-pluginParams", String.join(",", args.pluginParams))
            );
        });
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(Vaa3dStitchProcessor.class, new Vaa3dStitchArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {

            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }

            @Override
            public File collectResult(JacsServiceData jacsServiceData) {
                return getOutputFile(getArgs(jacsServiceData)).toFile();
            }
        };
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return vaa3dPluginProcessor.getErrorChecker();
    }

    @Override
    public ServiceComputation<JacsServiceResult<File>> process(JacsServiceData jacsServiceData) {
        return vaa3dPluginProcessor.process(jacsServiceData)
                .thenApply(v3dResult -> updateServiceResult(jacsServiceData, getResultHandler().collectResult(jacsServiceData)))
                ;
    }

    private Vaa3dStitchArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new Vaa3dStitchArgs());
    }

    private Path getOutputFile(Vaa3dStitchArgs args) {
        return Paths.get(args.inputDir).resolve(STITCHED_IMAGE_INFO_FILENAME);
    }
}

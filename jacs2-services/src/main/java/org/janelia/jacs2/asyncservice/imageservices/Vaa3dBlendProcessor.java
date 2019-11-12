package org.janelia.jacs2.asyncservice.imageservices;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Named;

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

@Named("vaa3dBlend")
public class Vaa3dBlendProcessor extends AbstractServiceProcessor<File> {

    static class Vaa3dBlendArgs extends ServiceArgs {
        @Parameter(names = "-inputDir", description = "Input file", required = true)
        String inputDir;
        @Parameter(names = "-outputFile", description = "Output file")
        String outputFile;
        @Parameter(names = {"-p", "-pluginParams"}, description = "Other plugin parameters")
        List<String> pluginParams = new ArrayList<>();
    }

    private final DelegateServiceProcessor<Vaa3dPluginProcessor, List<File>> vaa3dPluginProcessor;

    @Inject
    Vaa3dBlendProcessor(ServiceComputationFactory computationFactory,
                        JacsServiceDataPersistence jacsServiceDataPersistence,
                        @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                        Vaa3dPluginProcessor vaa3dPluginProcessor,
                        Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.vaa3dPluginProcessor = new DelegateServiceProcessor<>(vaa3dPluginProcessor, jacsServiceData -> {
            Vaa3dBlendArgs args = getArgs(jacsServiceData);
            return ImmutableList.of(
                    new ServiceArg("-plugin", "ifusion.so"),
                    new ServiceArg("-pluginFunc", "iblender"),
                    new ServiceArg("-input", args.inputDir),
                    new ServiceArg("-output", args.outputFile),
                    new ServiceArg("-pluginParams", String.join(",", args.pluginParams))
            );
        });
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(Vaa3dBlendProcessor.class, new Vaa3dBlendArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {

            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return getOutputFile(getArgs(jacsServiceData)).toFile().exists();
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

    private Vaa3dBlendArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new Vaa3dBlendArgs());
    }

    private Path getOutputFile(Vaa3dBlendArgs args) {
        return Paths.get(args.outputFile);
    }
}

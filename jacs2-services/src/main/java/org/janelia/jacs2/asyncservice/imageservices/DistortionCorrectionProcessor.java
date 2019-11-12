package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractSingleFileServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.StringJoiner;

@Named("distortionCorrection")
public class DistortionCorrectionProcessor extends AbstractServiceProcessor<File> {

    static class DistortionCorrectionArgs extends ServiceArgs {
        @Parameter(names = "-inputFile", description = "Input file", required = true)
        String inputFile;
        @Parameter(names = "-outputFile", description = "Output file", required = true)
        String outputFile;
        @Parameter(names = "-microscope", description = "Microscope name", required = true)
        String microscope;
    }

    private final String distortionCorrectionMacro;
    private final WrappedServiceProcessor<FijiMacroProcessor, Void> fijiMacroProcessor;

    @Inject
    DistortionCorrectionProcessor(ServiceComputationFactory computationFactory,
                                  JacsServiceDataPersistence jacsServiceDataPersistence,
                                  @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                  @PropertyValue(name = "Fiji.DistortionCorrection") String distortionCorrectionMacro,
                                  FijiMacroProcessor fijiMacroProcessor,
                                  Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.distortionCorrectionMacro = distortionCorrectionMacro;
        this.fijiMacroProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, fijiMacroProcessor);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(DistortionCorrectionProcessor.class, new DistortionCorrectionArgs());
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
    public ServiceComputation<JacsServiceResult<File>> process(JacsServiceData jacsServiceData) {
        DistortionCorrectionArgs args = getArgs(jacsServiceData);
        return fijiMacroProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                        .build(),
                new ServiceArg("-macro", distortionCorrectionMacro),
                new ServiceArg("-macroArgs", getMacroArgs(args)),
                new ServiceArg("-finalOutput", getOutputDir(args).toString()),
                new ServiceArg("-resultsPatterns", new File(args.outputFile).getName())
        ).thenApply(voidResult -> updateServiceResult(jacsServiceData, getResultHandler().collectResult(jacsServiceData)));
    }

    private String getMacroArgs(DistortionCorrectionArgs args) {
        StringJoiner builder = new StringJoiner(",");
        builder.add(String.format("%s/", getInputDir(args)));
        builder.add(String.format("%s", getInputFile(args).getFileName()));
        builder.add(String.format("%s/", getOutputDir(args)));
        builder.add(StringUtils.wrap(args.microscope, '"'));
        return builder.toString();
    }

    private DistortionCorrectionArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new DistortionCorrectionArgs());
    }

    private Path getInputFile(DistortionCorrectionArgs args) {
        return Paths.get(args.inputFile).toAbsolutePath();
    }

    private Path getInputDir(DistortionCorrectionArgs args) {
        return getInputFile(args).getParent();
    }

    private Path getOutputFile(DistortionCorrectionArgs args) {
        return Paths.get(args.outputFile);
    }

    private Path getOutputDir(DistortionCorrectionArgs args) {
        return getOutputFile(args).getParent();
    }
}

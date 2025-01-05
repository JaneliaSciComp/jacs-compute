package org.janelia.jacs2.asyncservice.imageservices;

import java.io.File;
import java.nio.file.Files;
import java.util.StringJoiner;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
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
@Named("vaa3dConverter")
public class Vaa3dConverterProcessor extends AbstractServiceProcessor<File> {

    static class Vaa3dConverterArgs extends ServiceArgs {
        @Parameter(names = "-inputFile", description = "Input file", required = true)
        String inputFileName;
        @Parameter(names = "-outputFile", description = "Output file", required = true)
        String outputFileName;
    }

    private final DelegateServiceProcessor<Vaa3dCmdProcessor, Void> vaa3dCmdProcessor;

    @Inject
    Vaa3dConverterProcessor(ServiceComputationFactory computationFactory,
                            JacsServiceDataPersistence jacsServiceDataPersistence,
                            @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                            Vaa3dCmdProcessor vaa3dCmdProcessor,
                            Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.vaa3dCmdProcessor = new DelegateServiceProcessor<>(vaa3dCmdProcessor, jacsServiceData -> {
            Vaa3dConverterArgs args = getArgs(jacsServiceData);
            StringJoiner vaa3dCmdArgs = new StringJoiner(" ")
                    .add("-convert")
                    .add(args.inputFileName)
                    .add(args.outputFileName);
            return ImmutableList.of(
                    new ServiceArg("-vaa3dCmd", "image-loader"),
                    new ServiceArg("-vaa3dCmdArgs", vaa3dCmdArgs.toString())
            );
        });

    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(Vaa3dConverterProcessor.class, new Vaa3dConverterArgs());
    }

    @Override
    public ServiceResultHandler<File> getResultHandler() {
        return new AbstractSingleFileServiceResultHandler() {

            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                Vaa3dConverterArgs args = getArgs(jacsServiceData);
                File outputFile = new File(args.outputFileName);
                return outputFile.exists();
            }

            @Override
            public File collectResult(JacsServiceData jacsServiceData) {
                Vaa3dConverterArgs args = getArgs(jacsServiceData);
                return new File(args.outputFileName);
            }
        };
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return vaa3dCmdProcessor.getErrorChecker();
    }

    @Override
    public ServiceComputation<JacsServiceResult<File>> process(JacsServiceData jacsServiceData) {
        try {
            Vaa3dConverterArgs args = getArgs(jacsServiceData);
            if (StringUtils.isBlank(args.inputFileName)) {
                throw new ComputationException(jacsServiceData, "Input file name must be specified");
            } else if (StringUtils.isBlank(args.outputFileName)) {
                throw new ComputationException(jacsServiceData, "Output file name must be specified");
            } else {
                File outputFile = new File(args.outputFileName);
                Files.createDirectories(outputFile.getParentFile().toPath());
            }
        } catch (ComputationException e) {
            throw e;
        } catch (Exception e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return vaa3dCmdProcessor.process(jacsServiceData)
                .thenApply(v3dResult -> updateServiceResult(jacsServiceData, getResultHandler().collectResult(jacsServiceData)))
                ;
    }

    private Vaa3dConverterArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new Vaa3dConverterArgs());
    }

}

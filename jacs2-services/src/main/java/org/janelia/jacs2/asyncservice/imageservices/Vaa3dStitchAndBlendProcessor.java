package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
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
import org.janelia.jacs2.asyncservice.fileservices.FileMoveProcessor;
import org.janelia.jacs2.asyncservice.fileservices.FileRemoveProcessor;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Named("vaa3dStitchAndBlend")
public class Vaa3dStitchAndBlendProcessor extends AbstractBasicLifeCycleServiceProcessor<StitchAndBlendResult, Vaa3dStitchAndBlendProcessor.StitchAndBlendIntermediateResult> {

    private static final java.lang.String DEFAULT_BLEND_OUTPUT = "output.v3draw";

    static class StitchAndBlendIntermediateResult {
        private final Number stichingServiceId;

        public StitchAndBlendIntermediateResult(Number stichingServiceId) {
            this.stichingServiceId = stichingServiceId;
        }
    }

    static class Vaa3dStitchAndBlendArgs extends ServiceArgs {
        @Parameter(names = "-inputDir", description = "Input file", required = true)
        String inputDir;
        @Parameter(names = "-outputFile", description = "Output file", required = true)
        String outputFile;
        @Parameter(names = "-refchannel", description = "Reference channel")
        int referenceChannel = 4;
        @Parameter(names = {"-otherStitchParams"}, description = "Other stitch plugin parameters")
        List<String> otherStitchPluginParams = new ArrayList<>();
        @Parameter(names = {"-otherBlendParams"}, description = "Other blend plugin parameters")
        List<String> otherBlendPluginParams = new ArrayList<>();
    }

    private final Vaa3dStitchProcessor vaa3dStitchProcessor;
    private final Vaa3dBlendProcessor vaa3dBlendProcessor;
    private final Vaa3dConverterProcessor vaa3dConverterProcessor;
    private final FileMoveProcessor fileMoveProcessor;
    private final FileRemoveProcessor fileRemoveProcessor;

    @Inject
    Vaa3dStitchAndBlendProcessor(ServiceComputationFactory computationFactory,
                                 JacsServiceDataPersistence jacsServiceDataPersistence,
                                 @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                 Vaa3dStitchProcessor vaa3dStitchProcessor,
                                 Vaa3dBlendProcessor vaa3dBlendProcessor,
                                 Vaa3dConverterProcessor vaa3dConverterProcessor,
                                 FileMoveProcessor fileMoveProcessor,
                                 FileRemoveProcessor fileRemoveProcessor,
                                 Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.vaa3dStitchProcessor = vaa3dStitchProcessor;
        this.vaa3dBlendProcessor = vaa3dBlendProcessor;
        this.vaa3dConverterProcessor = vaa3dConverterProcessor;
        this.fileMoveProcessor = fileMoveProcessor;
        this.fileRemoveProcessor = fileRemoveProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(Vaa3dStitchAndBlendProcessor.class, new Vaa3dStitchAndBlendArgs());
    }

    @Override
    public ServiceResultHandler<StitchAndBlendResult> getResultHandler() {

        return new AbstractAnyServiceResultHandler<StitchAndBlendResult>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return getOutputFile(getArgs(depResults.getJacsServiceData())).toFile().exists();
            }

            @Override
            public StitchAndBlendResult collectResult(JacsServiceResult<?> depResults) {
                StitchAndBlendIntermediateResult intermediateResult = (StitchAndBlendIntermediateResult) depResults.getResult();
                JacsServiceData stitchServiceData = jacsServiceDataPersistence.findById(intermediateResult.stichingServiceId);
                StitchAndBlendResult result = new StitchAndBlendResult();
                result.setStitchedImageInfoFile(vaa3dStitchProcessor.getResultHandler().getServiceDataResult(stitchServiceData));
                result.setStitchedFile(getOutputFile(getArgs(depResults.getJacsServiceData())).toFile());
                return result;
            }

            @Override
            public StitchAndBlendResult getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<StitchAndBlendResult>() {});
            }

        };
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        try {
            Vaa3dStitchAndBlendArgs args = getArgs(jacsServiceData);
            Files.createDirectories(getOutputFile(args).getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected JacsServiceResult<StitchAndBlendIntermediateResult> submitServiceDependencies(JacsServiceData jacsServiceData) {
        Vaa3dStitchAndBlendArgs args = getArgs(jacsServiceData);
        JacsServiceData stitchServiceData = stitch(
                getInputDir(args),
                args.referenceChannel,
                ImmutableList.<String>builder().add("#si 0").addAll(args.otherStitchPluginParams).build(),
                "Stitch data",
                jacsServiceData
        );
        Path temporaryBlendOutput = getTemporaryBlendOutput(args);
        JacsServiceData blendServiceData = blend(
                getInputDir(args),
                temporaryBlendOutput,
                ImmutableList.<String>builder().add("#s 1").addAll(args.otherBlendPluginParams).build(),
                "Blend results",
                jacsServiceData,
                stitchServiceData
        );
        Path outputFile = getOutputFile(args);
        if (".v3draw".equals(FileUtils.getFileExtensionOnly(args.outputFile))) {
            // if the output is a v3draw move the result
            mv(temporaryBlendOutput, outputFile, "Moving temporary output results", jacsServiceData, blendServiceData);
        } else {
            // if the output is not a v3draw convert the temporary v3draw file to the desired output
            JacsServiceData convertResultServiceData = convert(temporaryBlendOutput, outputFile, "Convert temporary output results", jacsServiceData, blendServiceData);
            rm(temporaryBlendOutput, "Removing temporary output results", jacsServiceData, convertResultServiceData);
        }
        return new JacsServiceResult<>(jacsServiceData, new StitchAndBlendIntermediateResult(stitchServiceData.getId()));
    }

    private JacsServiceData stitch(Path inputDir, int referenceChannel, List<String> additionalPluginParams, String description, JacsServiceData jacsServiceData, JacsServiceData... deps) {
        JacsServiceData stitchServiceData = vaa3dStitchProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .waitFor(deps)
                        .description(description)
                        .build(),
                new ServiceArg("-inputDir", inputDir.toString()),
                new ServiceArg("-refchannel", String.valueOf(referenceChannel)),
                new ServiceArg("-pluginParams", String.join(" ", additionalPluginParams))
        );
        return submitDependencyIfNotFound(stitchServiceData);
    }

    private JacsServiceData blend(Path inputDir, Path outputFile, List<String> additionalPluginParams, String description, JacsServiceData jacsServiceData, JacsServiceData... deps) {
        JacsServiceData blendServiceData = vaa3dBlendProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .waitFor(deps)
                        .description(description)
                        .build(),
                new ServiceArg("-inputDir", inputDir.toString()),
                new ServiceArg("-outputFile", outputFile.toString()),
                new ServiceArg("-pluginParams", String.join(" ", additionalPluginParams))
        );
        return submitDependencyIfNotFound(blendServiceData);
    }

    private JacsServiceData mv(Path source, Path target, String description, JacsServiceData jacsServiceData, JacsServiceData... deps) {
        JacsServiceData mvServiceData = fileMoveProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .waitFor(deps)
                        .description(description)
                        .build(),
                new ServiceArg("-source", source.toString()),
                new ServiceArg("-target", target.toString())
        );
        return submitDependencyIfNotFound(mvServiceData);
    }

    private JacsServiceData convert(Path source, Path target, String description, JacsServiceData jacsServiceData, JacsServiceData... deps) {
        JacsServiceData convertServiceData = vaa3dConverterProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .waitFor(deps)
                        .description(description)
                        .build(),
                new ServiceArg("-inputFile", source.toString()),
                new ServiceArg("-outputFile", target.toString())
        );
        return submitDependencyIfNotFound(convertServiceData);
    }

    private JacsServiceData rm(Path dataFile, String description, JacsServiceData jacsServiceData, JacsServiceData... deps) {
        JacsServiceData rmServiceData = fileRemoveProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .waitFor(deps)
                        .description(description)
                        .build(),
                new ServiceArg("-file", dataFile.toString())
        );
        return submitDependencyIfNotFound(rmServiceData);
    }

    @Override
    protected ServiceComputation<JacsServiceResult<StitchAndBlendIntermediateResult>> processing(JacsServiceResult<StitchAndBlendIntermediateResult> depResults) {
        return computationFactory.newCompletedComputation(depResults);
    }

    private Vaa3dStitchAndBlendArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new Vaa3dStitchAndBlendArgs());
    }

    private Path getInputDir(Vaa3dStitchAndBlendArgs args) {
        return Paths.get(args.inputDir);
    }

    private Path getTemporaryBlendOutput(Vaa3dStitchAndBlendArgs args) {
        return FileUtils.getFilePath(getInputDir(args), DEFAULT_BLEND_OUTPUT);
    }

    private Path getOutputFile(Vaa3dStitchAndBlendArgs args) {
        return Paths.get(args.outputFile);
    }

}

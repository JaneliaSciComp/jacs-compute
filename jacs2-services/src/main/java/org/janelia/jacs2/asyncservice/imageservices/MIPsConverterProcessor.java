package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Named("mipsConverter")
public class MIPsConverterProcessor extends AbstractServiceProcessor<List<MIPsConverterProcessor.MIPsResult>> {

    private static final String MIP_ARTIFACT_SUFFIX = "_mipArtifact";
    private static final String TIFF_EXTENSION = ".tif";
    private static final String PNG_EXTENSION = ".png";

    static class MIPsConverterArgs extends ServiceArgs {
        @Parameter(names = "-inputFiles", description = "List of input files for which to generate mips")
        List<String> inputFiles = new ArrayList<>();
        @Parameter(names = "-outputDir", description = "MIPs output directory")
        String outputDir;

        MIPsConverterArgs() {
            super("Service which takes a list of LSMs, TIFF or VAA3D files and generates the corresponding PNG MIPs");
        }
    }

    public static class MIPsResult {
        String inputFile;
        String outputMIPsFile;

        @JsonCreator
        public MIPsResult(String inputFile, String outputMIPsFile) {
            this.inputFile = inputFile;
            this.outputMIPsFile = outputMIPsFile;
        }

        public String getInputFile() {
            return inputFile;
        }

        public String getOutputMIPsFile() {
            return outputMIPsFile;
        }
    }
    private final WrappedServiceProcessor<Vaa3dMipCmdProcessor, List<File>> vaa3dMipCmdProcessor;
    private final WrappedServiceProcessor<ImageMagickConverterProcessor, List<File>> imageMagickConverterProcessor;

    @Inject
    MIPsConverterProcessor(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           Vaa3dMipCmdProcessor vaa3dMipCmdProcessor,
                           ImageMagickConverterProcessor imageMagickConverterProcessor,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.vaa3dMipCmdProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, vaa3dMipCmdProcessor);
        this.imageMagickConverterProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, imageMagickConverterProcessor);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(MIPsConverterProcessor.class, new MIPsConverterArgs());
    }

    @Override
    public ServiceResultHandler<List<MIPsResult>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<MIPsResult>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public List<MIPsResult> collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<List<MIPsResult>> intermediateResult = (JacsServiceResult<List<MIPsResult>>)depResults;
                return intermediateResult.getResult();
            }

            public List<MIPsResult> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<MIPsResult>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<MIPsResult>>> process(JacsServiceData jacsServiceData) {
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenCompose(sd -> createMipsComputation(sd))
                .thenApply(sr -> updateServiceResult(sr.getJacsServiceData(), sr.getResult()))
                ;
    }

    private MIPsConverterArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new MIPsConverterArgs());
    }

    @SuppressWarnings("unchecked")
    private ServiceComputation<JacsServiceResult<List<MIPsResult>>> createMipsComputation(JacsServiceData jacsServiceData) {
        MIPsConverterArgs args = getArgs(jacsServiceData);
        if (CollectionUtils.isEmpty(args.inputFiles)) {
            return computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData, Collections.emptyList()));
        } else {
            List<MIPsResult> mipsInputs = prepareMipsInput(args, jacsServiceData);
            return vaa3dMipCmdProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                            .description("Generate mips")
                            .build(),
                    new ServiceArg("-inputFiles",
                            mipsInputs.stream()
                                    .map((MIPsResult mipSource) -> mipSource.inputFile)
                                    .reduce((p1, p2) -> p1 + "," + p2)
                                    .orElse("")
                    ),
                    new ServiceArg("-outputFiles",
                            mipsInputs.stream()
                                    .map((MIPsResult mipSource) -> mipSource.outputMIPsFile)
                                    .reduce((p1, p2) -> p1 + "," + p2)
                                    .orElse("")
                    ))
                    .thenCompose(tifMipsResults -> imageMagickConverterProcessor.process(new ServiceExecutionContext.Builder(jacsServiceData)
                                    .description("Convert mips to png")
                                    .waitFor(tifMipsResults.getJacsServiceData())
                                    .build(),
                            new ServiceArg("-inputFiles",
                                    tifMipsResults.getResult().stream()
                                            .map((File tifMipResultFile) -> tifMipResultFile.getAbsolutePath())
                                            .reduce((p1, p2) -> p1 + "," + p2)
                                            .orElse("")
                            ),
                            new ServiceArg("-outputFiles",
                                    tifMipsResults.getResult().stream()
                                            .map((File tifMipResultFile) -> FileUtils.replaceFileExt(tifMipResultFile.toPath(), PNG_EXTENSION).toString())
                                            .reduce((p1, p2) -> p1 + "," + p2)
                                            .orElse("")
                            )))
                    .thenApply(pngMipsResults -> {
                        // find the correct mapping of the original input to the PNG result
                        Map<String, String> tif2pngConversions = pngMipsResults.getResult().stream()
                                .map((File pngMipResultFile) -> {
                                    Path converterInput = FileUtils.replaceFileExt(pngMipResultFile.toPath(), TIFF_EXTENSION);
                                    try {
                                        logger.debug("Delete TIFF file {} after it was converted to PNG: {}", converterInput, pngMipResultFile);
                                        FileUtils.deletePath(converterInput);
                                    } catch (IOException e) {
                                        logger.warn("Error deleting {}", converterInput, e);
                                    }
                                    return ImmutablePair.of(converterInput.toString(), pngMipResultFile.getAbsolutePath());
                                })
                                .collect(Collectors.toMap(tif2png -> tif2png.getLeft(), tif2png -> tif2png.getRight()));
                        List<MIPsResult> mipsResults = mipsInputs.stream()
                                .map(mipsInput -> new MIPsResult(mipsInput.inputFile, tif2pngConversions.get(mipsInput.outputMIPsFile)))
                                .collect(Collectors.toList());
                        return new JacsServiceResult<>(pngMipsResults.getJacsServiceData(), mipsResults);
                    });
        }
    }

    private List<MIPsResult> prepareMipsInput(MIPsConverterArgs args, JacsServiceData jacsServiceData) {
        JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
        Path intermediateResultsDir = serviceWorkingFolder.getServiceFolder("tempTifs");
        return args.inputFiles.stream()
                .map(inputName -> {
                    String tifMipsName = FileUtils.getFileNameOnly(inputName) + MIP_ARTIFACT_SUFFIX + TIFF_EXTENSION;
                    return new MIPsResult(inputName, intermediateResultsDir.resolve(tifMipsName).toString());
                })
                .collect(Collectors.toList());
    }
}

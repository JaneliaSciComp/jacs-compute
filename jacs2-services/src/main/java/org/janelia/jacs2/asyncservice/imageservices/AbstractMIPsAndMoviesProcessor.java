package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
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
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractMIPsAndMoviesProcessor extends AbstractServiceProcessor<MIPsAndMoviesResult> {

    protected static final String DEFAULT_OPTIONS = "mips:movies";

    protected static class MIPsAndMoviesArgs extends ServiceArgs {
        @Parameter(names = "-imgFile", description = "The name of the image file", required = true)
        String imageFile;
        @Parameter(names = "-imgFilePrefix", description = "Image file prefix", required = false)
        String imageFilePrefix;
        @Parameter(names = "-secondImgFile", description = "The name of the image file", required = false)
        String secondImageFile;
        @Parameter(names = "-secondImgFilePrefix", description = "Second image file prefix", required = false)
        String secondImageFilePrefix;
        @Parameter(names = "-mode", description = "Mode")
        String mode = "none";
        @Parameter(names = "-chanSpec", description = "Channel spec", required = true)
        String chanSpec;
        @Parameter(names = "-colorSpec", description = "Color spec", required = false)
        String colorSpec;
        @Parameter(names = "-divSpec", description = "Color spec", required = false)
        String divSpec;
        @Parameter(names = "-laser", description = "Laser", required = false)
        Integer laser;
        @Parameter(names = "-gain", description = "Gain", required = false)
        Integer gain;
        @Parameter(names = "-resultsDir", description = "Results directory", required = false)
        String resultsDir;
        @Parameter(names = "-options", description = "Options", required = false)
        String options = "mips:movies:legends:bcomp";
    }

    private final String mipsAndMoviesMacro;
    private final String scratchLocation;
    private final WrappedServiceProcessor<FijiMacroProcessor, Void> fijiMacroProcessor;
    private final WrappedServiceProcessor<VideoFormatConverterProcessor, File> mpegConverterProcessor;

    AbstractMIPsAndMoviesProcessor(ServiceComputationFactory computationFactory,
                                   JacsServiceDataPersistence jacsServiceDataPersistence,
                                   String defaultWorkingDir,
                                   String mipsAndMoviesMacro,
                                   String scratchLocation,
                                   FijiMacroProcessor fijiMacroProcessor,
                                   VideoFormatConverterProcessor mpegConverterProcessor,
                                   Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.mipsAndMoviesMacro = mipsAndMoviesMacro;
        this.scratchLocation =scratchLocation;
        this.fijiMacroProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, fijiMacroProcessor);
        this.mpegConverterProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, mpegConverterProcessor);
    }

    @Override
    public ServiceResultHandler<MIPsAndMoviesResult> getResultHandler() {
        return new AbstractAnyServiceResultHandler<MIPsAndMoviesResult>() {
            final String resultsPattern = "glob:**/*.{png,avi,mp4}";

            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                // don't count the files because this locks if there are no results
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public MIPsAndMoviesResult collectResult(JacsServiceResult<?> depResults) {
                MIPsAndMoviesArgs args = getArgs(depResults.getJacsServiceData());
                MIPsAndMoviesResult result = new MIPsAndMoviesResult();
                result.setResultsDir(getResultsDir(args).toString());
                FileUtils.lookupFiles(getResultsDir(args), 1, resultsPattern)
                        .map(Path::toString)
                        .forEach(result::addFile);
                return result;
            }

            @Override
            public MIPsAndMoviesResult getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<MIPsAndMoviesResult>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<MIPsAndMoviesResult>> process(JacsServiceData jacsServiceData) {
        MIPsAndMoviesArgs args = getArgs(jacsServiceData);
        try {
            Files.createDirectories(getResultsDir(args));
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return processFijiService(jacsServiceData, args)
                .thenCompose((JacsServiceResult<Void> voidResult) -> {
                    final String aviResults = "glob:**/*.avi";
                    // collect generated AVIs and convert them to MPEGs
                    List<ServiceComputation<?>> avi2MpegComputations =
                            FileUtils.lookupFiles(getResultsDir(args), 1, aviResults)
                                    .map((Path aviPath) -> mpegConverterProcessor.process(
                                                    new ServiceExecutionContext.Builder(jacsServiceData)
                                                            .description("Convert AVI to MPEG")
                                                            .addRequiredMemoryInGB(getRequiredMemoryInGB())
                                                            .waitFor(voidResult.getJacsServiceData())
                                                            .build(),
                                                    new ServiceArg("-input", aviPath.toString())
                                            ).thenApply(pd -> {
                                                try {
                                                    FileUtils.deletePath(aviPath);
                                                } catch (IOException e) {
                                                    logger.warn("Error removing {}", aviPath, e);
                                                }
                                                return pd.getResult();
                                            })
                                    )
                                    .collect(Collectors.toList());
                    return computationFactory
                            .newCompletedComputation(voidResult)
                            .thenCombineAll(avi2MpegComputations, (JacsServiceResult<Void> vr, List<?> mpegResults) -> (List<File>) mpegResults);
                })
                .thenSuspendUntil(this.suspendCondition(jacsServiceData))
                .thenApply(mpegResults -> this.updateServiceResult(jacsServiceData, this.getResultHandler().collectResult(new JacsServiceResult<Void>(jacsServiceData))))
                .thenApply(this::removeTempDir)
                ;
    }

    private ServiceComputation<JacsServiceResult<Void>> processFijiService(JacsServiceData jacsServiceData, MIPsAndMoviesArgs args) {
        if (StringUtils.isBlank(args.chanSpec)) {
            throw new ComputationException(jacsServiceData,  "Channel spec is required for " + jacsServiceData.toString());
        }
        Path temporaryOutputDir = getTemporaryOutput(args, jacsServiceData);
        return fijiMacroProcessor.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Invoke Fiji macro")
                        .addRequiredMemoryInGB(getRequiredMemoryInGB())
                        .build(),
                new ServiceArg("-macro", mipsAndMoviesMacro),
                new ServiceArg("-macroArgs", getMIPsAndMoviesArgs(args, temporaryOutputDir)),
                new ServiceArg("-temporaryOutput", temporaryOutputDir.toString()),
                new ServiceArg("-finalOutput", getResultsDir(args).toString()),
                new ServiceArg("-headless", false),
                new ServiceArg("-resultsPatterns", "*.properties"),
                new ServiceArg("-resultsPatterns", "*.png"),
                new ServiceArg("-resultsPatterns", "*.avi")
        );
    }

    private Path getTemporaryOutput(MIPsAndMoviesArgs args, JacsServiceData jacsServiceData) {
        JacsServiceFolder scratchFolder = getScratchFolder(jacsServiceData);
        return scratchFolder.getServiceFolder(FileUtils.getFileNameOnly(args.imageFile));
    }

    private JacsServiceFolder getScratchFolder(JacsServiceData jacsServiceData) {
        return new JacsServiceFolder(null, Paths.get(scratchLocation), jacsServiceData);
    }

    protected abstract String getMIPsAndMoviesArgs(MIPsAndMoviesArgs args, Path outputDir);

    private JacsServiceResult<MIPsAndMoviesResult> removeTempDir(JacsServiceResult<MIPsAndMoviesResult> sr) {
        try {
            JacsServiceFolder scratchFolder = getScratchFolder(sr.getJacsServiceData());
            FileUtils.deletePath(scratchFolder.getServiceFolder());
            return sr;
        } catch (Exception e) {
            throw new ComputationException(sr.getJacsServiceData(), e);
        }
    }

    protected MIPsAndMoviesArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new MIPsAndMoviesArgs());
    }

    protected Path getResultsDir(MIPsAndMoviesArgs args) {
        return Paths.get(args.resultsDir);
    }

    private int getRequiredMemoryInGB() {
        return 50;
    }
}

package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class AbstractMIPsAndMoviesProcessor extends AbstractBasicLifeCycleServiceProcessor<JacsServiceData, MIPsAndMoviesResult> {

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
    private final FijiMacroProcessor fijiMacroProcessor;
    private final VideoFormatConverterProcessor mpegConverterProcessor;

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
        this.fijiMacroProcessor = fijiMacroProcessor;
        this.mpegConverterProcessor = mpegConverterProcessor;
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
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        MIPsAndMoviesArgs args = getArgs(jacsServiceData);
        try {
            Files.createDirectories(getResultsDir(args));
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected JacsServiceResult<JacsServiceData> submitServiceDependencies(JacsServiceData jacsServiceData) {
        MIPsAndMoviesArgs args = getArgs(jacsServiceData);
        JacsServiceData fijiServiceData = submitFijiService(args, jacsServiceData);
        logger.debug("Submitted FIJI service {}", fijiServiceData);
        return new JacsServiceResult<>(jacsServiceData, fijiServiceData);
    }

    private JacsServiceData submitFijiService(MIPsAndMoviesArgs args, JacsServiceData jacsServiceData) {
        if (StringUtils.isBlank(args.chanSpec)) {
            throw new ComputationException(jacsServiceData,  "Channel spec is required for " + jacsServiceData.toString());
        }
        Path temporaryOutputDir = getTemporaryOutput(args, jacsServiceData);
        JacsServiceData fijiMacroService = fijiMacroProcessor.createServiceData(
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
        logger.debug("Submit FIJI service {}", fijiMacroService);
        return submitDependencyIfNotFound(fijiMacroService);
    }

    protected Path getTemporaryOutput(MIPsAndMoviesArgs args, JacsServiceData jacsServiceData) {
        return getServicePath(scratchLocation, jacsServiceData, FileUtils.getFileNameOnly(args.imageFile));
    }

    protected abstract String getMIPsAndMoviesArgs(MIPsAndMoviesArgs args, Path outputDir);

    @Override
    protected ServiceComputation<JacsServiceResult<JacsServiceData>> processing(JacsServiceResult<JacsServiceData> depResults) {
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    MIPsAndMoviesResult result = getResultHandler().collectResult(pd);
                    result.getFileList().stream()
                            .filter(f -> f.endsWith(".avi"))
                            .forEach(f -> submitMpegConverterService(f, "Convert AVI to MPEG", pd.getJacsServiceData(), pd.getResult()));
                    return pd;
                });
    }

    private JacsServiceData submitMpegConverterService(String aviFileName, String description, JacsServiceData jacsServiceData, JacsServiceData dep) {
        JacsServiceData mpegConverterService = mpegConverterProcessor.createServiceData(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description(description)
                        .addRequiredMemoryInGB(getRequiredMemoryInGB())
                        .waitFor(dep)
                        .build(),
                new ServiceArg("-input", aviFileName)
        );
        return submitDependencyIfNotFound(mpegConverterService);
    }

    @Override
    protected JacsServiceResult<MIPsAndMoviesResult> postProcessing(JacsServiceResult<MIPsAndMoviesResult> sr) {
        try {
            Path temporaryOutputDir = getServicePath(scratchLocation, sr.getJacsServiceData());
            FileUtils.deletePath(temporaryOutputDir);
            return sr;
        } catch (Exception e) {
            throw new ComputationException(sr.getJacsServiceData(), e);
        }
    }

    protected MIPsAndMoviesArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(jacsServiceData.getArgsArray(), new MIPsAndMoviesArgs());
    }

    protected Path getResultsDir(MIPsAndMoviesArgs args) {
        return Paths.get(args.resultsDir);
    }

    private int getRequiredMemoryInGB() {
        return 50;
    }
}

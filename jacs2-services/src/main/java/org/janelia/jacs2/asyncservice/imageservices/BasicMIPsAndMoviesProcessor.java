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
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

@Named("basicMIPsAndMovies")
public class BasicMIPsAndMoviesProcessor extends AbstractBasicLifeCycleServiceProcessor<JacsServiceData, BasicMIPsAndMoviesResult> {

    static class BasicMIPsAndMoviesArgs extends ServiceArgs {
        @Parameter(names = "-imgFile", description = "The name of the image file", required = true)
        String imageFile;
        @Parameter(names = "-chanSpec", description = "Channel spec", required = true)
        String chanSpec;
        @Parameter(names = "-colorSpec", description = "Color spec", required = false)
        String colorSpec;
        @Parameter(names = "-laser", description = "Laser", required = false)
        Integer laser;
        @Parameter(names = "-gain", description = "Gain", required = false)
        Integer gain;
        @Parameter(names = "-resultsDir", description = "Results directory", required = false)
        String resultsDir;
        @Parameter(names = "-options", description = "Options", required = false)
        String options = "mips:movies:legends:bcomp";
    }

    private static final String DEFAULT_OPTIONS = "mips:movies";

    private final String basicMIPsAndMoviesMacro;
    private final String scratchLocation;
    private final FijiMacroProcessor fijiMacroProcessor;
    private final VideoFormatConverterProcessor mpegConverterProcessor;

    @Inject
    BasicMIPsAndMoviesProcessor(ServiceComputationFactory computationFactory,
                                JacsServiceDataPersistence jacsServiceDataPersistence,
                                @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                @PropertyValue(name = "Fiji.BasicMIPsAndMovies") String basicMIPsAndMoviesMacro,
                                @PropertyValue(name = "service.DefaultScratchDir") String scratchLocation,
                                FijiMacroProcessor fijiMacroProcessor,
                                VideoFormatConverterProcessor mpegConverterProcessor,
                                Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.basicMIPsAndMoviesMacro = basicMIPsAndMoviesMacro;
        this.scratchLocation =scratchLocation;
        this.fijiMacroProcessor = fijiMacroProcessor;
        this.mpegConverterProcessor = mpegConverterProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(this.getClass(), new BasicMIPsAndMoviesArgs());
    }

    @Override
    public ServiceResultHandler<BasicMIPsAndMoviesResult> getResultHandler() {
        return new AbstractAnyServiceResultHandler<BasicMIPsAndMoviesResult>() {
            final String resultsPattern = "glob:**/*.{png,avi,mp4}";

            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                // don't count the files because this locks if there are no results
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public BasicMIPsAndMoviesResult collectResult(JacsServiceResult<?> depResults) {
                BasicMIPsAndMoviesArgs args = getArgs(depResults.getJacsServiceData());
                BasicMIPsAndMoviesResult result = new BasicMIPsAndMoviesResult();
                result.setResultsDir(getResultsDir(args).toString());
                FileUtils.lookupFiles(getResultsDir(args), 1, resultsPattern)
                        .map(Path::toFile)
                        .forEach(result::addFile);
                return result;
            }

            @Override
            public BasicMIPsAndMoviesResult getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<BasicMIPsAndMoviesResult>() {});
            }
        };
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        BasicMIPsAndMoviesArgs args = getArgs(jacsServiceData);
        try {
            Files.createDirectories(getResultsDir(args));
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected JacsServiceResult<JacsServiceData> submitServiceDependencies(JacsServiceData jacsServiceData) {
        BasicMIPsAndMoviesArgs args = getArgs(jacsServiceData);
        JacsServiceData fijiServiceData = submitFijiService(args, jacsServiceData);
        return new JacsServiceResult<>(jacsServiceData, fijiServiceData);
    }

    private JacsServiceData submitFijiService(BasicMIPsAndMoviesArgs args, JacsServiceData jacsServiceData) {
        if (StringUtils.isBlank(args.chanSpec)) {
            throw new ComputationException(jacsServiceData,  "No channel spec for " + args.imageFile);
        }
        Path temporaryOutputDir = getServicePath(scratchLocation, jacsServiceData, FileUtils.getFileNameOnly(args.imageFile));
        JacsServiceData fijiMacroService = fijiMacroProcessor.createServiceData(new ServiceExecutionContext(jacsServiceData),
                new ServiceArg("-macro", basicMIPsAndMoviesMacro),
                new ServiceArg("-macroArgs", getBasicMIPsAndMoviesArgs(args, temporaryOutputDir)),
                new ServiceArg("-temporaryOutput", temporaryOutputDir.toString()),
                new ServiceArg("-finalOutput", getResultsDir(args).toString()),
                new ServiceArg("-headless", true),
                new ServiceArg("-resultsPatterns", "*.properties"),
                new ServiceArg("-resultsPatterns", "*.png"),
                new ServiceArg("-resultsPatterns", "*.avi")
        );
        jacsServiceDataPersistence.saveHierarchy(fijiMacroService);
        return fijiMacroService;
    }

    private String getBasicMIPsAndMoviesArgs(BasicMIPsAndMoviesArgs args, Path outputDir) {
        List<FijiColor> colors = FijiUtils.getColorSpec(args.colorSpec, args.chanSpec);
        if (colors.isEmpty()) {
            colors = FijiUtils.getDefaultColorSpec(args.chanSpec, "RGB", '1');
        }
        String colorSpec = colors.stream().map(c -> String.valueOf(c.getCode())).collect(Collectors.joining(""));
        String divSpec = colors.stream().map(c -> String.valueOf(c.getDivisor())).collect(Collectors.joining(""));
        StringJoiner builder = new StringJoiner(",");
        builder.add(outputDir.toString()); // output directory
        builder.add(FileUtils.getFileNameOnly(args.imageFile)); // output prefix 1
        builder.add(""); // output prefix 2
        builder.add(args.imageFile); // input file 1
        builder.add(""); // input file 2
        builder.add(args.laser == null ? "" : args.laser.toString());
        builder.add(args.gain == null ? "" : args.gain.toString());
        builder.add(args.chanSpec);
        builder.add(colorSpec);
        builder.add(divSpec);
        builder.add(StringUtils.defaultIfBlank(args.options, DEFAULT_OPTIONS));
        return builder.toString();
    }

    @Override
    protected ServiceComputation<JacsServiceResult<JacsServiceData>> processing(JacsServiceResult<JacsServiceData> depResults) {
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    BasicMIPsAndMoviesResult result = getResultHandler().collectResult(pd);
                    result.getFileList().stream()
                            .filter(f -> f.getName().endsWith(".avi"))
                            .forEach(f -> submitMpegConverterService(f, "Convert AVI to MPEG", pd.getJacsServiceData(), pd.getResult()));
                    return pd;
                });
    }

    private JacsServiceData submitMpegConverterService(File aviFile, String description, JacsServiceData jacsServiceData, JacsServiceData dep) {
        JacsServiceData mpegConverterService = mpegConverterProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                .description(description)
                .waitFor(dep)
                .build(),
                new ServiceArg("-input", aviFile.getAbsolutePath())
        );
        return submitDependencyIfNotPresent(jacsServiceData, mpegConverterService);
    }

    @Override
    protected BasicMIPsAndMoviesResult postProcessing(JacsServiceResult<BasicMIPsAndMoviesResult> sr) {
        try {
            Path temporaryOutputDir = getServicePath(scratchLocation, sr.getJacsServiceData());
            FileUtils.deletePath(temporaryOutputDir);
            return sr.getResult();
        } catch (Exception e) {
            throw new ComputationException(sr.getJacsServiceData(), e);
        }
    }

    private BasicMIPsAndMoviesArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(jacsServiceData.getArgsArray(), new BasicMIPsAndMoviesArgs());
    }

    private Path getResultsDir(BasicMIPsAndMoviesArgs args) {
        return Paths.get(args.resultsDir, FileUtils.getFileNameOnly(args.imageFile));
    }

}

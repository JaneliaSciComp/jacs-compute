package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.DefaultServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractFileListServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Create a square montage from given PNGs assuming a tile pattern with the given number of tiles per side. If the number of tiles per side is not specified
 * it tries to form a square from the list of provided images.
 */
@Named("imageMagickConverter")
public class ImageMagickConverterProcessor extends AbstractExeBasedServiceProcessor<List<File>> {

    private static final String DEFAULT_CONVERSION_EXT = ".png";

    static class ImageConverterArgs extends ServiceArgs {
        @Parameter(names = "-inputFiles", description = "List of input files to be converted.", required = true)
        List<String> inputFiles;
        @Parameter(names = "-outputFiles", description = "List of output files")
        List<String> outputFiles;
    }

    private final String convertToolLocation;
    private final String convertToolName;
    private final String libraryPath;

    @Inject
    ImageMagickConverterProcessor(ServiceComputationFactory computationFactory,
                                  JacsServiceDataPersistence jacsServiceDataPersistence,
                                  @Any Instance<ExternalProcessRunner> serviceRunners,
                                  @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                  @PropertyValue(name = "ImageMagick.Bin.Path") String convertToolLocation,
                                  @PropertyValue(name = "ImageMagick.Convert.Name") String convertToolName,
                                  @PropertyValue(name = "ImageMagick.Lib.Path") String libraryPath,
                                  JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                                  @ApplicationProperties ApplicationConfig applicationConfig,
                                  Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.convertToolLocation = convertToolLocation;
        this.convertToolName = convertToolName;
        this.libraryPath = libraryPath;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(ImageMagickConverterProcessor.class, new ImageConverterArgs());
    }

    @Override
    public ServiceResultHandler<List<File>> getResultHandler() {
        return new AbstractFileListServiceResultHandler() {

            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                ImageConverterArgs args = getArgs(depResults.getJacsServiceData());
                return getConverterArgs(args)
                        .map(converterArgs -> Files.exists(converterArgs.getRight()))
                        .reduce((f1, f2) -> f1 && f2)
                        .orElse(true); // if the list is empty then it's done
            }

            @Override
            public List<File> collectResult(JacsServiceResult<?> depResults) {
                ImageConverterArgs args = getArgs(depResults.getJacsServiceData());
                return getConverterArgs(args)
                        .map(converterArgs -> converterArgs.getRight().toFile())
                        .collect(Collectors.toList());
            }
        };
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return new DefaultServiceErrorChecker(logger) {
            @Override
            protected boolean hasErrors(String l) {
                if (l.matches("(?i:.*(Segmentation fault|core dumped).*)")) {
                    // core dump is still an error
                    logger.error(l);
                    return true;
                } else if (l.matches("(?i:.*(error/annotate\\.c).*)")) {
                    // I have seen this probably because of a missing font so I will ignore them for now.
                    logger.warn(l);
                    return false;
                } else if (l.matches("(?i:.*(error|exception).*)")) {
                    // but will consider any other error or exception
                    logger.error(l);
                    return true;
                } else {
                    return false;
                }
            }
        };
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        try {
            ImageConverterArgs args = getArgs(jacsServiceData);
            List<Pair<Path, Path>> converterArgs = getConverterArgs(args)
                    .filter(argsPair -> !argsPair.getLeft().equals(argsPair.getRight()))
                    .collect(Collectors.toList());
            if (converterArgs.isEmpty()) {
                throw new IllegalArgumentException("Final converter arguments list is empty - either there are no non-empty inputs or the inputs and outputs are the same: " + args);
            }
            converterArgs.stream()
                    .map(inOutArgs -> inOutArgs.getRight())
                    .forEach(outputPath -> {
                        try {
                            Files.createDirectories(outputPath.getParent());
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (ComputationException e) {
            throw e;
        } catch (Exception e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        externalScriptWriter.read("INPUT");
        externalScriptWriter.read("OUTPUT");
        externalScriptWriter.addWithArgs(getExecutable())
                .addArgs("${INPUT}", "${OUTPUT}")
                .endArgs("");
        externalScriptWriter.close();
        return externalScriptCode;
    }

    @Override
    protected List<ExternalCodeBlock> prepareConfigurationFiles(JacsServiceData jacsServiceData) {
        ImageConverterArgs args = getArgs(jacsServiceData);
        return getConverterArgs(args)
                .filter(argsPair -> !argsPair.getLeft().equals(argsPair.getRight()))
                .map(inOutArg -> {
                    ExternalCodeBlock configFileBlock = new ExternalCodeBlock();
                    ScriptWriter configWriter = configFileBlock.getCodeWriter();
                    configWriter.add(inOutArg.getLeft().toString());
                    configWriter.add(inOutArg.getRight().toString());
                    configWriter.close();
                    return configFileBlock;
                })
                .collect(Collectors.toList());
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        return ImmutableMap.of(DY_LIBRARY_PATH_VARNAME, getUpdatedEnvValue(DY_LIBRARY_PATH_VARNAME, getFullExecutableName(libraryPath)));
    }

    private ImageConverterArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new ImageConverterArgs());
    }

    private Stream<? extends Pair<Path, Path>> getConverterArgs(ImageConverterArgs args) {
        if (CollectionUtils.isEmpty(args.outputFiles)) {
            return args.inputFiles.stream()
                    .filter(fn -> StringUtils.isNotBlank(fn))
                    .map(fn -> {
                        Path inputFilePath = Paths.get(fn);
                        return ImmutablePair.of(inputFilePath, FileUtils.replaceFileExt(inputFilePath, DEFAULT_CONVERSION_EXT));
                    });
        } else {
            return Streams
                    .zip(args.inputFiles.stream(), args.outputFiles.stream(), (inputFileName, outputFileName) -> {
                        Path inputFilePath = null, outputFilePath = null;
                        if (StringUtils.isNotBlank(inputFileName)) {
                            inputFilePath = Paths.get(inputFileName);
                            if (StringUtils.isBlank(outputFileName)) {
                                outputFilePath = FileUtils.replaceFileExt(inputFilePath, DEFAULT_CONVERSION_EXT);
                            } else {
                                outputFilePath = Paths.get(outputFileName);
                            }
                        }
                        return ImmutablePair.of(inputFilePath, outputFilePath);
                    })
                    .filter(p -> p.getLeft() != null);
        }
    }

    private String getExecutable() {
        return getFullExecutableName(convertToolLocation, convertToolName);
    }

}

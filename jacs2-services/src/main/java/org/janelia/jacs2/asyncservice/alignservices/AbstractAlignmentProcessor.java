package org.janelia.jacs2.asyncservice.alignservices;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.DefaultServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.asyncservice.utils.X11Utils;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.service.JacsServiceData;
import org.slf4j.Logger;

import javax.enterprise.inject.Instance;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public abstract class AbstractAlignmentProcessor extends AbstractExeBasedServiceProcessor<AlignmentResultFiles> {

    private final String alignmentRunner;
    private final String libraryPath;
    private final String toolsDir;
    private final String alignmentConfigDir;
    private final String alignmentTemplatesDir;
    private final String alignmentScriptsDir;

    AbstractAlignmentProcessor(ServiceComputationFactory computationFactory,
                               JacsServiceDataPersistence jacsServiceDataPersistence,
                               Instance<ExternalProcessRunner> serviceRunners,
                               String defaultWorkingDir,
                               String alignmentRunner,
                               String alignmentScriptsDir,
                               String toolsDir,
                               String alignmentConfigDir,
                               String alignmentTemplatesDir,
                               String libraryPath,
                               JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                               ApplicationConfig applicationConfig,
                               Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.alignmentRunner = alignmentRunner;
        this.libraryPath = libraryPath;
        this.toolsDir = toolsDir;
        this.alignmentConfigDir = alignmentConfigDir;
        this.alignmentTemplatesDir = alignmentTemplatesDir;
        this.alignmentScriptsDir = alignmentScriptsDir;
    }

    @Override
    public ServiceResultHandler<AlignmentResultFiles> getResultHandler() {
        return new AbstractAnyServiceResultHandler<AlignmentResultFiles>() {
            final String resultsPattern = "glob:**/{Align,QiScore,rotations,Affine,ccmi,VerifyMovie}*";

            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public AlignmentResultFiles collectResult(JacsServiceResult<?> depResults) {
                AlignmentArgs args = getArgs(depResults.getJacsServiceData());
                AlignmentResultFiles result = new AlignmentResultFiles();
                Path resultDir = getOutputDir(args);
                result.setAlgorithm(args.alignmentAlgorithm);
                result.setResultDir(resultDir.toString());
                FileUtils.lookupFiles(resultDir, 3, resultsPattern)
                        .forEach(f -> {
                            String fn = f.toFile().getName();
                            if (fn.endsWith(".properties")) {
                                result.setAlignmentPropertiesFile(f.toString());
                            } else if ("QiScore.csv".equals(fn)) {
                                result.setScoresFile(resultDir.relativize(f).toString());
                            } else if (fn.endsWith(".mp4")) {
                                // if this is an mp4 file assume it's the verification movie
                                result.setAlignmentVerificationMovie(resultDir.relativize(f).toString());
                            }
                        });
                return result;
            }

            @Override
            public AlignmentResultFiles getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<AlignmentResultFiles>() {});
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
                } else if (l.matches("(?i:.*(fail to call the plugin).*)")) {
                    // vaa3d plugin call failed
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
        AlignmentArgs args = getArgs(jacsServiceData);
        AlignmentInput firstInput = getAlignmentFirstInput(args);
        if (firstInput.isEmpty()) {
            throw new IllegalArgumentException("Alignment input cannot be empty - either input1 or input1File must be defined");
        }
        try {
            Path outputDir = getOutputDir(args);
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        AlignmentArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        addStartX11ServerCmd(jacsServiceData, externalScriptWriter);
        createScript(args, externalScriptWriter);
        externalScriptWriter.close();
        return externalScriptCode;
    }

    private void addStartX11ServerCmd(JacsServiceData jacsServiceData, ScriptWriter scriptWriter) {
        try {
            Path workingDir = getWorkingDirectory(jacsServiceData).getServiceFolder();
            X11Utils.setDisplayPort(workingDir.toString(), scriptWriter);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void createScript(AlignmentArgs args, ScriptWriter scriptWriter) {
        scriptWriter.addWithArgs(getExecutable())
                .addArg(getAlignmentScript(args))
                .addArg(String.valueOf(args.numThreads))
                .addArgFlag("-o", getOutputDir(args).toString())
                .addArgFlag("-c", getAlignmentConfigDir())
                .addArgFlag("-t", getAlignmentTemplatesDir())
                .addArgFlag("-k", getToolsDir())
                .addArgFlag("-g", args.gender)
                .addArgFlag("-i", getFirstInput(args))
                .endArgs("");
    }

    private String getFirstInput(AlignmentArgs args) {
        if (StringUtils.isNotBlank(args.input1)) {
            return args.input1;
        } else {
            return AlignmentUtils.formatInput(mapArgsToAlignmentInput(args.input1File, args.input1Channels, args.input1Ref, args.input1Res, args.input1Dims));
        }
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        AlignmentArgs args = getArgs(jacsServiceData);
        return ImmutableMap.of(
                DY_LIBRARY_PATH_VARNAME, getUpdatedEnvValue(DY_LIBRARY_PATH_VARNAME, libraryPath),
                "ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS", String.valueOf(args.numThreads),
                "FSLOUTPUTTYPE", args.fslOutputType
        );
    }

    protected AlignmentArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new AlignmentArgs());
    }

    protected AlignmentInput getAlignmentFirstInput(AlignmentArgs args) {
        AlignmentInput alignmentInput;
        if (StringUtils.isNotBlank(args.input1File)) {
            alignmentInput = mapArgsToAlignmentInput(args.input1File, args.input1Channels, args.input1Ref, args.input1Res, args.input1Dims);
        } else {
            alignmentInput = AlignmentUtils.parseInput(args.input1);
        }
        return alignmentInput;
    }

    private AlignmentInput mapArgsToAlignmentInput(String inputFile, int inputChannelsCount, int referenceChannelPos, String resolution, String dimensions) {
        AlignmentInput alignmentInput = new AlignmentInput();
        alignmentInput.name = inputFile;
        alignmentInput.channels = String.valueOf(inputChannelsCount);
        alignmentInput.ref = String.valueOf(referenceChannelPos);
        alignmentInput.res = resolution;
        alignmentInput.dims = dimensions;
        return alignmentInput;
    }

    protected Path getOutputDir(AlignmentArgs args) {
        return Paths.get(args.outputDir);
    }

    protected String getToolsDir() {
        return getFullExecutableName(toolsDir);
    }

    protected String getAlignmentTemplatesDir() {
        return getFullExecutableName(alignmentTemplatesDir);
    }

    protected String getAlignmentConfigDir() {
        return getFullExecutableName(alignmentConfigDir);
    }

    private String getExecutable() {
        return getFullExecutableName(alignmentRunner);
    }

    private String getAlignmentScript(AlignmentArgs args) {
        Preconditions.checkArgument(StringUtils.isNotBlank(args.alignmentAlgorithm), "No alignment algorithm has been specified");
        String alignmentScript;
        if (args.alignmentAlgorithm.endsWith(".sh")) {
            alignmentScript = args.alignmentAlgorithm;
        } else {
            alignmentScript = args.alignmentAlgorithm + ".sh";
        }
        return getFullExecutableName(alignmentScriptsDir, alignmentScript);
    }
}

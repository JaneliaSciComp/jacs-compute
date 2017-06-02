package org.janelia.jacs2.asyncservice.alignservices;

import com.beust.jcommander.JCommander;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.ThrottledProcessesQueue;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.asyncservice.utils.X11Utils;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.slf4j.Logger;

import javax.enterprise.inject.Instance;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public abstract class AbstractExternalAlignmentProcessor extends AbstractExeBasedServiceProcessor<Void, Void> {

    private final String alignmentRunner;
    private final String libraryPath;
    private final String toolsDir;
    private final String alignmentScriptsDir;

    AbstractExternalAlignmentProcessor(ServiceComputationFactory computationFactory,
                                       JacsServiceDataPersistence jacsServiceDataPersistence,
                                       Instance<ExternalProcessRunner> serviceRunners,
                                       String defaultWorkingDir,
                                       String alignmentRunner,
                                       String alignmentScriptsDir,
                                       String toolsDir,
                                       String libraryPath,
                                       ThrottledProcessesQueue throttledProcessesQueue,
                                       ApplicationConfig applicationConfig,
                                       Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, throttledProcessesQueue, applicationConfig, logger);
        this.alignmentRunner = alignmentRunner;
        this.libraryPath = libraryPath;
        this.toolsDir = toolsDir;
        this.alignmentScriptsDir = alignmentScriptsDir;
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
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
            Path workingDir = getWorkingDirectory(jacsServiceData);
            X11Utils.setDisplayPort(workingDir.toString(), scriptWriter);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void createScript(AlignmentArgs args, ScriptWriter scriptWriter) {
        scriptWriter.addWithArgs(getExecutable())
                .addArg(getAlignmentScript(args))
                .addArg(String.valueOf(args.numThreads))
                .addArgFlag("-o", args.outputDir)
                .addArgFlag("-c", args.configFile)
                .addArgFlag("-t", args.templateDir)
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
        AlignmentArgs args = new AlignmentArgs();
        new JCommander(args).parse(jacsServiceData.getArgsArray());
        return args;
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

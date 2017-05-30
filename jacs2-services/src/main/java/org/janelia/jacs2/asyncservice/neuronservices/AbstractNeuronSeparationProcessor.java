package org.janelia.jacs2.asyncservice.neuronservices;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.ThrottledProcessesQueue;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.slf4j.Logger;

import javax.enterprise.inject.Instance;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public abstract class AbstractNeuronSeparationProcessor extends AbstractExeBasedServiceProcessor<Void, Void> {

    static class NeuronSeparationArgs extends ServiceArgs {
        @Parameter(names = {"-inputFile"}, description = "Input file name", required = true)
        String inputFile;
        @Parameter(names = {"-outputDir"}, description = "Output directory name", required = true)
        String outputDir;
        @Parameter(names = "-previousResultFile", description = "Previous result file name")
        String previousResultFile;
        @Parameter(names = "-signalChannels", description = "Signal channels")
        String signalChannels = "0 1 2";
        @Parameter(names = "-referenceChannel", description = "Reference channel")
        String referenceChannel = "3";
        @Parameter(names = "-numThreads", description = "Number of threads")
        int numThreads = 16;
    }

    private final String executable;
    private final String libraryPath;

    AbstractNeuronSeparationProcessor(ServiceComputationFactory computationFactory,
                                      JacsServiceDataPersistence jacsServiceDataPersistence,
                                      Instance<ExternalProcessRunner> serviceRunners,
                                      String defaultWorkingDir,
                                      String executable,
                                      String libraryPath,
                                      ThrottledProcessesQueue throttledProcessesQueue,
                                      ApplicationConfig applicationConfig,
                                      Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, throttledProcessesQueue, applicationConfig, logger);
        this.executable = executable;
        this.libraryPath = libraryPath;
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        NeuronSeparationArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        createScript(args, externalScriptWriter);
        externalScriptWriter.close();
        return externalScriptCode;
    }

    private void createScript(NeuronSeparationArgs args, ScriptWriter scriptWriter) {
        scriptWriter.addWithArgs(getExecutable())
                .addArg(args.outputDir)
                .addArg("neuronSeparatorPipeline")
                .addArg(args.inputFile)
                .addArg(StringUtils.wrap(args.signalChannels, '"'))
                .addArg(StringUtils.wrap(args.referenceChannel, '"'));
        if (StringUtils.isNotBlank(args.previousResultFile)) {
            scriptWriter.addArg(args.previousResultFile);
        }
        scriptWriter.endArgs("");
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        NeuronSeparationArgs args = getArgs(jacsServiceData);
        return ImmutableMap.of(
                DY_LIBRARY_PATH_VARNAME, getUpdatedEnvValue(DY_LIBRARY_PATH_VARNAME, libraryPath),
                "NFE_MAX_THREAD_COUNT", String.valueOf(args.numThreads)
        );
    }

    protected abstract NeuronSeparationArgs getArgs(JacsServiceData jacsServiceData);

    protected Path getOutputDir(NeuronSeparationArgs args) {
        if (StringUtils.isNotBlank(args.outputDir)) {
            return Paths.get(args.outputDir);
        } else {
            return null;
        }
    }

    private String getExecutable() {
        return getFullExecutableName(executable);
    }
}

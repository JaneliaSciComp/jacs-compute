package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.DefaultServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractFileListServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.asyncservice.utils.X11Utils;
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

/**
 * This is a MIP generation service based on NeuronSeparator/mipCreator.sh script.
 * The service generates separate MIPs for the signal and for the reference channels.
 */
@Named("signalAndReferenceChannelMIPs")
public class SignalAndReferenceChannelsMIPsProcessor extends AbstractExeBasedServiceProcessor<List<File>> {

    static class SignalAndReferenceChannelsMIPsArgs extends ServiceArgs {
        @Parameter(names = "-inputFile", description = "The name of the input file", required = true)
        String inputFile;
        @Parameter(names = "-signalChannels", description = "Zero based space or comma separated signal channels")
        String signalChannels;
        @Parameter(names = "-referenceChannel", description = "Zero based reference channel", required = false)
        String referenceChannel;
        @Parameter(names = "-outputDir", description = "Output directory", required = true)
        String outputDir;
        @Parameter(names = "-imgFormat", description = "Image format: {jpg, png}", required = false)
        String imgFormat = "png";

        SignalAndReferenceChannelsMIPsArgs() {
            super("Generate separate MIPs for the signal and reference channels from input file");
        }
    }

    private final String executable;
    private final String libraryPath;

    @Inject
    SignalAndReferenceChannelsMIPsProcessor(ServiceComputationFactory computationFactory,
                                            JacsServiceDataPersistence jacsServiceDataPersistence,
                                            @Any Instance<ExternalProcessRunner> serviceRunners,
                                            @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                            @PropertyValue(name = "MipCreator.ScriptPath") String executable,
                                            @PropertyValue(name = "VAA3D.Library.Path") String libraryPath,
                                            JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                                            @ApplicationProperties ApplicationConfig applicationConfig,
                                            Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.executable = executable;
        this.libraryPath = libraryPath;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(SignalAndReferenceChannelsMIPsProcessor.class, new SignalAndReferenceChannelsMIPsArgs());
    }

    @Override
    public ServiceResultHandler<List<File>> getResultHandler() {
        return new AbstractFileListServiceResultHandler() {

            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                // don't count the files because this locks if there are no results
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public List<File> collectResult(JacsServiceResult<?> depResults) {
                SignalAndReferenceChannelsMIPsArgs args = getArgs(depResults.getJacsServiceData());
                Path outputDir = getOutputDir(args);
                String pattern = String.format("glob:**/*{_signal,_reference}.%s", getOutputFileExt(args));
                return FileUtils.lookupFiles(outputDir, 1, pattern)
                        .map(Path::toFile)
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
        SignalAndReferenceChannelsMIPsArgs args = getArgs(jacsServiceData);
        try {
            Files.createDirectories(getOutputDir(args));
        } catch (IOException e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        SignalAndReferenceChannelsMIPsArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        createScript(jacsServiceData, args, externalScriptCode.getCodeWriter());
        return externalScriptCode;
    }

    private void createScript(JacsServiceData jacsServiceData, SignalAndReferenceChannelsMIPsArgs args, ScriptWriter scriptWriter) {
        try {
            JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
            X11Utils.setDisplayPort(serviceWorkingFolder.getServiceFolder().toString(), scriptWriter);
            scriptWriter.addWithArgs(getExecutable())
                    .addArg(getOutputDir(args).toString())
                    .addArg(args.imgFormat)
                    .addArg(args.inputFile)
                    .addArg(StringUtils.wrap(getSignalChannels(args), '"'))
                    .addArg(StringUtils.wrap(StringUtils.defaultIfBlank(args.referenceChannel, ""), '"'))
                    .endArgs("");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        return ImmutableMap.of(
                DY_LIBRARY_PATH_VARNAME, getUpdatedEnvValue(DY_LIBRARY_PATH_VARNAME, libraryPath),
                "NFE_MAX_THREAD_COUNT", String.valueOf(ProcessorHelper.getProcessingSlots(jacsServiceData.getResources()))
        );
    }

    private SignalAndReferenceChannelsMIPsArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new SignalAndReferenceChannelsMIPsArgs());
    }

    private String getExecutable() {
        return getFullExecutableName(executable);
    }

    private Path getOutputDir(SignalAndReferenceChannelsMIPsArgs args) {
        return Paths.get(args.outputDir).toAbsolutePath();
    }

    private String getSignalChannels(SignalAndReferenceChannelsMIPsArgs args) {
        String signalChannels = args.signalChannels;
        if (StringUtils.isBlank(signalChannels)) {
            return "";
        }
        List<String> channelsList = Splitter.on(CharMatcher.anyOf(" ,\t")).splitToList(signalChannels.trim());
        return channelsList.isEmpty() ? "" : String.join(" ", channelsList);
    }

    private String getOutputFileExt(SignalAndReferenceChannelsMIPsArgs args) {
        return args.imgFormat;
    }
}

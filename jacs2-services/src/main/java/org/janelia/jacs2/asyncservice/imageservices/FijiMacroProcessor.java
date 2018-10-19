package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.CoreDumpServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceErrorChecker;
import org.janelia.jacs2.asyncservice.utils.ScriptUtils;
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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Named("fijiMacro")
public class FijiMacroProcessor extends AbstractExeBasedServiceProcessor<Void> {

    static class FijiMacroArgs extends ServiceArgs {
        @Parameter(names = "-macro", description = "FIJI macro name", required = true)
        String macroName;
        @Parameter(names = "-macroArgs", description = "Arguments for the fiji macro")
        List<String> macroArgs;
        @Parameter(names = "-temporaryOutput", description = "Temporary output directory")
        String temporaryOutput;
        @Parameter(names = "-finalOutput", description = "Final output directory")
        String finalOutput;
        @Parameter(names = "-headless", description = "Run Fiji in headless mode")
        boolean headless;
        @Parameter(names = "-resultsPatterns", description = "results patterns")
        List<String> resultsPatterns = new ArrayList<>();
    }

    private final String fijiExecutable;
    private final String fijiMacrosPath;

    @Inject
    FijiMacroProcessor(ServiceComputationFactory computationFactory,
                       JacsServiceDataPersistence jacsServiceDataPersistence,
                       @Any Instance<ExternalProcessRunner> serviceRunners,
                       @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                       @PropertyValue(name = "Fiji.Bin.Path") String fijiExecutable,
                       @PropertyValue(name = "Fiji.Macro.Path") String fijiMacrosPath,
                       JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                       @ApplicationProperties ApplicationConfig applicationConfig,
                       Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.fijiExecutable = fijiExecutable;
        this.fijiMacrosPath = fijiMacrosPath;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(FijiMacroProcessor.class, new FijiMacroArgs());
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return new CoreDumpServiceErrorChecker(logger);
    }

    @Override
    protected JacsServiceData prepareProcessing(JacsServiceData jacsServiceData) {
        try {
            FijiMacroArgs args = getArgs(jacsServiceData);
            Path temporaryOutput = getTemporaryDir(args);
            if (temporaryOutput != null) {
                Files.createDirectories(temporaryOutput);
            }
        } catch (Exception e) {
            throw new ComputationException(jacsServiceData, e);
        }
        return super.prepareProcessing(jacsServiceData);
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        FijiMacroArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        ScriptWriter externalScriptWriter = externalScriptCode.getCodeWriter();
        createScript(jacsServiceData, args, externalScriptWriter);
        externalScriptWriter.close();
        return externalScriptCode;
    }

    private void createScript(JacsServiceData jacsServiceData, FijiMacroArgs args, ScriptWriter scriptWriter) {
        try {
            boolean headless = args.headless || getApplicationConfig().getBooleanPropertyValue("Fiji.RunHeadless");
            if (StringUtils.isNotBlank(args.temporaryOutput)) {
                Files.createDirectories(Paths.get(args.temporaryOutput));
            }
            if (StringUtils.isNotBlank(args.finalOutput)) {
                Files.createDirectories(Paths.get(args.finalOutput));
            }
            JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
            if (!headless) {
                X11Utils.setDisplayPort(serviceWorkingFolder.getServiceFolder().toString(), scriptWriter);
            }
            // Create temp dir so that large temporary avis are not created on the network drive
            JacsServiceFolder scratchServiceFolder;
            if (StringUtils.isBlank(args.temporaryOutput)) {
                scratchServiceFolder = serviceWorkingFolder;
            } else {
                scratchServiceFolder = new JacsServiceFolder(null, Paths.get(args.temporaryOutput), jacsServiceData);
            }
            Path scratchDir = scratchServiceFolder.getServiceFolder();
            Files.createDirectories(scratchDir);
            ScriptUtils.createTempDir("cleanTemp", scratchDir.toString(), scriptWriter);
            // define the exit handlers
            scriptWriter
                    .add("function exitHandler() { cleanXvfb; cleanTemp; }")
                    .add("trap exitHandler EXIT\n");

            scriptWriter.addWithArgs(getFijiExecutable());
            if (headless) {
                scriptWriter.addArg("--headless");
            }
            scriptWriter
                    .addArg("-macro").addArg(getFullFijiMacro(args))
                    .addArg(String.join(",", args.macroArgs));
            if (!headless) {
                scriptWriter.endArgs("&");
                // Monitor Fiji and take periodic screenshots, killing it eventually
                scriptWriter.setVar("fpid", "$!");
                X11Utils.startScreenCaptureLoop(scratchDir + "/xvfb-" + jacsServiceData.getId() + ".${PORT}",
                        "PORT", "fpid", 30, getTimeoutInSeconds(jacsServiceData), scriptWriter);
            } else {
                scriptWriter.endArgs("");
            }
            if (StringUtils.isNotBlank(args.finalOutput) && StringUtils.isNotBlank(args.temporaryOutput) &&
                    !args.finalOutput.equals(args.temporaryOutput)) {
                // the copy should not fail if the file exists
                if (args.resultsPatterns.isEmpty()) {
                    scriptWriter.add(String.format("mv %s/* %s || true", args.temporaryOutput, args.finalOutput));
                } else {
                    args.resultsPatterns.forEach(resultPattern -> scriptWriter.add(String.format("mv %s/%s %s || true", args.temporaryOutput, resultPattern, args.finalOutput)));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        return ImmutableMap.of();
    }

    private FijiMacroArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new FijiMacroArgs());
    }

    private String getFijiExecutable() {
        return getFullExecutableName(fijiExecutable);
    }

    private String getFullFijiMacro(FijiMacroArgs args) {
        if (args.macroName.startsWith("/")) {
            return args.macroName;
        } else {
            return getFullExecutableName(fijiMacrosPath, args.macroName);
        }
    }

    private int getTimeoutInSeconds(JacsServiceData sd) {
        long timeoutInMillis = sd.timeout();
        if (timeoutInMillis > 0) {
            return (int) timeoutInMillis / 1000;
        } else {
            return X11Utils.DEFAULT_TIMEOUT_SECONDS;
        }
    }

    private Path getTemporaryDir(FijiMacroArgs args) {
        if (StringUtils.isNotBlank(args.temporaryOutput)) {
            return Paths.get(args.temporaryOutput);
        } else {
            return null;
        }
    }
}

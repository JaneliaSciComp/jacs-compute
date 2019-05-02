package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.cluster.ComputeAccounting;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.BoolPropertyValue;
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
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

@Named("deconvolution")
public class DeconvolutionProcessor extends AbstractExeBasedServiceProcessor<Void> {

    static class DeconvolutionArgs extends ServiceArgs {
        @Parameter(names = {"-i", "-tileChannelConfigurationFiles"},
                   description = "Path to the input tile configuration files. Each configuration corresponds to one channel.")
        List<String> tileChannelConfigurationFiles = new ArrayList<>();
        @Parameter(names = {"-p", "-psfFiles"}, description = "Path to the files containing point spread functions. Each psf file correspond to one channel and there must be a 1:1 correspondence with input configuration files", required = true)
        List<String> psfFiles = new ArrayList<>();
        @Parameter(names = {"-z", "-psfZStep"}, description = "PSF Z step in microns.")
        Float psfZStep;
        @Parameter(names = {"-n", "-numIterations"}, description = "Number of deconvolution iterations.")
        Integer nIterations = 10;
        @Parameter(names = {"-v", "-backgroundValue"}, description = "Background intensity value which will be subtracted from the data and the PSF (one per input channel). If omitted, the pivot value estimated in the Flatfield Correction step will be used (default).")
        Float backgroundValue;
        @Parameter(names = {"-c", "-coresPerTask"}, description = "Number of CPU cores used by a single decon task.")
        Integer coresPerTask;

        DeconvolutionArgs() {
            super("Image deconvolution processor");
        }

        void validate() {
            if (tileChannelConfigurationFiles.size() != psfFiles.size()) {
                throw new IllegalArgumentException("Tile configuration files and psf files must have the same size - " +
                        "tileConfigurationFiles: " + tileChannelConfigurationFiles.size() + ", psfFile: " + psfFiles.size());
            }
        }
    }

    private final ObjectMapper objectMapper;
    private final String executableScript;
    private final boolean requiresAccountInfo;

    @Inject
    DeconvolutionProcessor(ServiceComputationFactory computationFactory,
                           JacsServiceDataPersistence jacsServiceDataPersistence,
                           @Any Instance<ExternalProcessRunner> serviceRunners,
                           @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                           JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                           ApplicationConfig applicationConfig,
                           ObjectMapper objectMapper,
                           @PropertyValue(name= "Deconvolution.Script.Path") String executableScript,
                           @BoolPropertyValue(name = "service.cluster.requiresAccountInfo", defaultValue = true) boolean requiresAccountInfo,
                           Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.objectMapper = objectMapper;
        this.executableScript = executableScript;
        this.requiresAccountInfo = requiresAccountInfo;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(DeconvolutionProcessor.class, new DeconvolutionArgs());
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        DeconvolutionArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        createScript(jacsServiceData, args, externalScriptCode.getCodeWriter());
        return externalScriptCode;
    }

    private void createScript(JacsServiceData jacsServiceData, DeconvolutionArgs args, ScriptWriter scriptWriter) {
        if (StringUtils.isNotBlank(pythonPath)) {
            scriptWriter.addWithArgs(pythonPath);
            scriptWriter.addArg(getFullExecutableName(executableScript));
        } else {
            scriptWriter.addWithArgs(getFullExecutableName(executableScript));
        }
        scriptWriter.addArg("-i").addArgs(args.tileChannelConfigurationFiles);
        scriptWriter.addArg("-p").addArgs(args.psfFiles);
        if (args.psfZStep != null) {
            scriptWriter.addArgs("-z", args.psfZStep.toString());
        }
        if (args.nIterations != null && args.nIterations > 0) {
            scriptWriter.addArgs("-n", args.nIterations.toString());
        }
        if (args.backgroundValue != null) {
            scriptWriter.addArgs("-v", args.backgroundValue.toString());
        }
        if (args.coresPerTask != null && args.coresPerTask > 0) {
            scriptWriter.addArgs("-c", args.coresPerTask.toString());
        }
        if (requiresAccountInfo) {
            scriptWriter.addArgs("--lsfproject", accounting.getComputeAccount(jacsServiceData));
        }
        scriptWriter.endArgs();
    }

    private DeconvolutionArgs getArgs(JacsServiceData jacsServiceData) {
        DeconvolutionArgs args = ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new DeconvolutionArgs());
        args.validate();
        return args;
    }

    private <T> Optional<T> loadJsonConfiguration(String jsonFileName, TypeReference<T> typeReference) {
        if (StringUtils.isBlank(jsonFileName)) {
            return Optional.empty();
        } else {
            InputStream jsonInputStream;
            try {
                jsonInputStream = new FileInputStream(jsonFileName);
                return Optional.of(objectMapper.readValue(jsonInputStream, typeReference));
            } catch (Exception e) {
                logger.error("Error reading json config from {}", jsonFileName, e);
                throw new IllegalStateException("Error reading json config from " + jsonFileName, e);
            }
        }
    }

    private String getFlatfieldFileName(String channelConfigFile) {
        Path channelConfigFilePath = Paths.get(channelConfigFile);
        Path channelConfigDir;
        if (channelConfigFilePath.getParent() == null) {
            channelConfigDir = Paths.get("");
        } else {
            channelConfigDir = channelConfigFilePath.getParent();
        }
        String channelConfigFileName = FileUtils.getFileNameOnly(channelConfigFilePath);
        return Stream.of("-flatfield", "-n5-flatfield")
                .map(flatfieldSuffix -> channelConfigDir.resolve(channelConfigFileName + flatfieldSuffix))
                .filter(flatfieldPath -> FileUtils.fileExists(flatfieldPath))
                .findFirst()
                .map(fp -> fp.toString())
                .orElse(null);
    }

    private void prepareJobConfigs(DeconvolutionArgs args) {
        Streams.zip(args.tileChannelConfigurationFiles.stream(), args.psfFiles.stream(), (channelConfigFile, psfFile) -> {
            Map<String, Object> channelConfig = loadJsonConfiguration(channelConfigFile);
            Map<String, Object> flatFieldConfig = loadJsonConfiguration(getFlatfieldFileName(channelConfigFile));
            Float backgroundIntensity;
            if (args.backgroundValue != null) {
                backgroundIntensity = args.backgroundValue;
            } else {
                Number pivotValue = (Number)flatFieldConfig.get("pivotValue");
                if (pivotValue != null) {
                    backgroundIntensity = pivotValue.floatValue();
                }
            }
        });
    }
}

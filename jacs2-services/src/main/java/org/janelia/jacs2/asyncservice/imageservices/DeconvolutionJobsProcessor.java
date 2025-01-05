package org.janelia.jacs2.asyncservice.imageservices;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.jacs2.asyncservice.common.AbstractExeBasedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.ExternalProcessRunner;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.access.dao.JacsJobInstanceInfoDao;
import org.janelia.model.jacs2.domain.IndexedReference;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

@Dependent
@Named("deconvolutionJobs")
public class DeconvolutionJobsProcessor extends AbstractExeBasedServiceProcessor<Void> {

    private final String matlabRootDir;
    private final List<String> matlabLibRelativeDirs;
    private final String matlabX11LibRelativeDir;
    private final String deconvolutionExecutable;
    private final DeconvolutionHelper deconvolutionHelper;

    @Inject
    DeconvolutionJobsProcessor(ServiceComputationFactory computationFactory,
                               JacsServiceDataPersistence jacsServiceDataPersistence,
                               @Any Instance<ExternalProcessRunner> serviceRunners,
                               @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                               JacsJobInstanceInfoDao jacsJobInstanceInfoDao,
                               @ApplicationProperties ApplicationConfig applicationConfig,
                               ObjectMapper objectMapper,
                               @PropertyValue(name= "Matlab.Root.Path") String matlabRootDir,
                               @PropertyValue(name = "Matlab.Lib.RelativePaths") String matlabLibRelativeDirs,
                               @PropertyValue(name = "Matlab.X11Lib.RelativePath") String matlabX11LibRelativeDir,
                               @PropertyValue(name= "Deconvolution.Script.Path") String deconvolutionExecutable,
                               Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, serviceRunners, defaultWorkingDir, jacsJobInstanceInfoDao, applicationConfig, logger);
        this.matlabRootDir = StringUtils.defaultIfBlank(matlabRootDir, "");
        this.matlabLibRelativeDirs = StringUtils.isNotBlank(matlabLibRelativeDirs)
                ? Splitter.on(',').trimResults().omitEmptyStrings().splitToList(matlabLibRelativeDirs)
                : ImmutableList.of();
        this.matlabX11LibRelativeDir = matlabX11LibRelativeDir;
        this.deconvolutionExecutable = deconvolutionExecutable;
        this.deconvolutionHelper = new DeconvolutionHelper(objectMapper);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(DeconvolutionJobsProcessor.class, new DeconvolutionArgs());
    }

    @Override
    protected Map<String, String> prepareEnvironment(JacsServiceData jacsServiceData) {
        String matlabLibPaths = matlabLibRelativeDirs.stream()
                .map(libRelDir -> Paths.get(matlabRootDir, libRelDir).toString())
                .reduce((p1, p2) -> p1 + LIBPATH_SEPARATOR + p2)
                .orElse("");
        String matlabX11LibDir = StringUtils.isNotBlank(matlabX11LibRelativeDir)
                ? Paths.get(matlabRootDir, matlabX11LibRelativeDir).toString()
                : "";
        return ImmutableMap.of("MATLAB_ROOT", matlabRootDir,
                DY_LIBRARY_PATH_VARNAME, getUpdatedEnvValue(DY_LIBRARY_PATH_VARNAME, matlabLibPaths),
                "XAPPLRESDIR", matlabX11LibDir,
                "MCR_INHIBIT_CTF_LOCK", "1");
    }

    @Override
    protected void prepareResources(JacsServiceData jacsServiceData) {
        DeconvolutionArgs args = getArgs(jacsServiceData);
        if (args.coresPerTask != null) {
            ProcessorHelper.setRequiredSlots(jacsServiceData.getResources(),
                    Math.max(ProcessorHelper.getRequiredSlots(jacsServiceData.getResources()), args.coresPerTask));
        }
    }

    @Override
    protected ExternalCodeBlock prepareExternalScript(JacsServiceData jacsServiceData) {
        DeconvolutionArgs args = getArgs(jacsServiceData);
        ExternalCodeBlock externalScriptCode = new ExternalCodeBlock();
        createScript(jacsServiceData, args, externalScriptCode.getCodeWriter());
        return externalScriptCode;
    }

    private void createScript(JacsServiceData jacsServiceData, DeconvolutionArgs args, ScriptWriter scriptWriter) {
        scriptWriter
                .read("tile_filepath")
                .read("output_tile_dir")
                .read("output_tile_filepath")
                .read("psf_filepath")
                .read("flatfield_dirpath")
                .read("background_value")
                .read("data_z_resolution")
                .read("psf_z_step")
                .read("num_iterations");
        // set MCR_CACHE
        scriptWriter
                .add("if [ -d /scratch ] ; then")
                .addIndent().exportVar("MCR_CACHE_ROOT", "/scratch/${USER}/mcr_cache_$$").removeIndent()
                .add("else")
                .addIndent().exportVar("MCR_CACHE_ROOT", "`mktemp -u`").removeIndent()
                .add("fi");
        // create output directory
        scriptWriter.add("umask 0002");
        scriptWriter.add("echo trying to create $output_tile_dir");
        scriptWriter.add("mkdir -p $output_tile_dir");
        // invoke matlab deconvolution tool
        scriptWriter
                .addWithArgs("echo")
                .addArg(getFullExecutableName(deconvolutionExecutable))
                .addArg("$tile_filepath")
                .addArg("$output_tile_filepath")
                .addArg("$psf_filepath")
                .addArg("$flatfield_dirpath")
                .addArg("$background_value")
                .addArg("$data_z_resolution")
                .addArg("$psf_z_step")
                .addArg("$num_iterations")
                .endArgs();
        scriptWriter
                .addWithArgs(getFullExecutableName(deconvolutionExecutable))
                .addArg("$tile_filepath")
                .addArg("$output_tile_filepath")
                .addArg("$psf_filepath")
                .addArg("$flatfield_dirpath")
                .addArg("$background_value")
                .addArg("$data_z_resolution")
                .addArg("$psf_z_step")
                .addArg("$num_iterations")
                .endArgs();
    }

    @Override
    protected List<ExternalCodeBlock> prepareConfigurationFiles(JacsServiceData jacsServiceData) {
        DeconvolutionArgs args = getArgs(jacsServiceData);
        return prepareJobConfigs(args)
                .map(instanceArgs -> {
                    ExternalCodeBlock instanceConfig = new ExternalCodeBlock();
                    ScriptWriter configWriter = instanceConfig.getCodeWriter();
                    configWriter.add(getTileTaskArg(instanceArgs, "tile_filepath"));
                    configWriter.add(getTileTaskArg(instanceArgs, "output_tile_dir"));
                    configWriter.add(getTileTaskArg(instanceArgs, "output_tile_filepath"));
                    configWriter.add(getTileTaskArg(instanceArgs, "psf_filepath"));
                    configWriter.add(getTileTaskArg(instanceArgs, "flatfield_dirpath"));
                    configWriter.add(getTileTaskArg(instanceArgs, "background_value"));
                    configWriter.add(getTileTaskArg(instanceArgs, "data_z_resolution"));
                    configWriter.add(getTileTaskArg(instanceArgs, "psf_z_step"));
                    configWriter.add(getTileTaskArg(instanceArgs, "num_iterations"));
                    configWriter.close();
                    return instanceConfig;
                })
                .collect(Collectors.toList());
    }

    private String getTileTaskArg(Map<String, String> tileTaskArgs, String argName) {
        return StringUtils.defaultIfBlank(tileTaskArgs.get(argName), "");
    }

    private DeconvolutionArgs getArgs(JacsServiceData jacsServiceData) {
        DeconvolutionArgs args = ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new DeconvolutionArgs());
        args.validate();
        return args;
    }

    private String getFlatfieldAttributesFileName(String channelConfigFile) {
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
                .map(fp -> fp.resolve("attributes.json").toString())
                .orElse(null);
    }

    @SuppressWarnings("unchecked")
    private Stream<Map<String, String>> prepareJobConfigs(DeconvolutionArgs args) {
        return IndexedReference.indexStream(
                Streams.zip(args.tileChannelConfigurationFiles.stream(), args.psfFiles.stream(), (channelConfigFile, psfFile) -> ImmutablePair.of(channelConfigFile, psfFile)),
                (channelIndex, channelConfigPair) -> new IndexedReference<>(channelConfigPair, channelIndex))
                .flatMap(indexedConfig -> {
                    int channelIndex = indexedConfig.getPos();
                    String channelConfigFile = indexedConfig.getReference().getLeft();
                    String psfFile = indexedConfig.getReference().getRight();
                    List<Map<String, Object>> channelTileConfigs = deconvolutionHelper.loadJsonConfiguration(channelConfigFile,
                            new TypeReference<List<Map<String, Object>>>() {}).orElseGet(ImmutableList::of);

                    String flatfieldAttributesFileName = getFlatfieldAttributesFileName(channelConfigFile);
                    Float backgroundIntensity;
                    if (args.backgroundValue != null) {
                        backgroundIntensity = args.backgroundValue;
                    } else {
                        Map<String, Object> flatFieldConfig = deconvolutionHelper.loadJsonConfiguration(flatfieldAttributesFileName,
                                new TypeReference<Map<String, Object>>() {}).orElseGet(ImmutableMap::of);
                        Number pivotValue = (Number)flatFieldConfig.get("pivotValue");
                        if (pivotValue != null) {
                            backgroundIntensity = pivotValue.floatValue();
                        } else {
                            backgroundIntensity = null;
                        }
                    }
                    String deconvOutputDir = deconvolutionHelper.mapToDeconvOutputDir(channelConfigFile);
                    int nIterations = args.getNumIterations(channelIndex);
                    return channelTileConfigs.stream()
                            .map(tileConfig -> {
                                Map<String, String> taskConfig = new LinkedHashMap<>();
                                List<Number> pixelResolutions = (List<Number>) tileConfig.get("pixelResolution");
                                taskConfig.put("tile_filepath", (String)tileConfig.get("file"));
                                taskConfig.put("output_tile_dir", deconvOutputDir);
                                taskConfig.put("output_tile_filepath", deconvolutionHelper.getTileDeconvFile(tileConfig, deconvOutputDir));
                                taskConfig.put("psf_filepath", psfFile);
                                taskConfig.put("flatfield_dirpath", FileUtils.getParent(flatfieldAttributesFileName));
                                taskConfig.put("background_value", backgroundIntensity != null ? backgroundIntensity.toString() : null);
                                taskConfig.put("data_z_resolution", pixelResolutions.get(2).toString());
                                taskConfig.put("psf_z_step", args.psfZStep != null ? args.psfZStep.toString() : null);
                                taskConfig.put("num_iterations", String.valueOf(nIterations));
                                return taskConfig;
                            });
                })
                ;
    }

}

package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ExternalCodeBlock;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.containerizedservices.SingularityContainerProcessor;
import org.janelia.jacs2.asyncservice.utils.ScriptWriter;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.jacs2.domain.IndexedReference;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.janelia.jacs2.utils.HttpUtils;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

/**
 * Service for running a single lightsheet processing step.
 *
 * @author David Ackerman
 */
@Named("lightsheetPipeline")
public class LightsheetPipelineStepProcessor extends AbstractServiceProcessor<Void> {

    private static final String DEFAULT_CONFIG_OUTPUT_PATH = "";

    static class LightsheetPipelineStepArgs extends ServiceArgs {
        @Parameter(names = "-step", description = "Which pipeline step to run", required = true)
        LightsheetPipelineStep step;
        @Parameter(names = "-stepIndex", description = "Which step index is this")
        Integer stepIndex = 1;
        @Parameter(names = "-containerImage", description = "Container image")
        String containerImage;
        @Parameter(names = "-numTimePoints", description = "Number of time points")
        Integer numTimePoints = 1;
        @Parameter(names = "-timePointsPerJob", description = "Number of time points per job")
        Integer timePointsPerJob = 1;
        @Parameter(names = "-configAddress", description = "Address for accessing job's config json", required = true)
        String configAddress;
        @Parameter(names = "-configOutputPath", description = "Path for outputting json configs", required=false)
        String configOutputPath = DEFAULT_CONFIG_OUTPUT_PATH;
    }

    private final SingularityContainerProcessor containerProcessor;
    private final ApplicationConfig applicationConfig;
    private final ObjectMapper objectMapper;

    @Inject
    LightsheetPipelineStepProcessor(ServiceComputationFactory computationFactory,
                                    JacsServiceDataPersistence jacsServiceDataPersistence,
                                    @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                    @ApplicationProperties ApplicationConfig applicationConfig,
                                    SingularityContainerProcessor containerProcessor,
                                    ObjectMapper objectMapper,
                                    Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.containerProcessor = containerProcessor;
        this.applicationConfig = applicationConfig;
        this.objectMapper = objectMapper;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LightsheetPipelineStepProcessor.class, new LightsheetPipelineStepArgs());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        try {
            LightsheetPipelineStepArgs args = getArgs(jacsServiceData);
            Pair<String, Map<String, Object>> stepConfig = getStepConfig(jacsServiceData, args);
            String stepConfigFile = stepConfig.getLeft();
            Path stepConfigPath = Paths.get(stepConfigFile).getParent();
            String containerLocation = getContainerLocation(stepConfig.getRight(), args);
            List<List<String>> stepJobArgs = getStepJobArgs(stepConfigFile, args);
            Map<String, String> stepResources = prepareResources(args, jacsServiceData.getResources());
            List<ServiceComputation<?>> stepJobs = IndexedReference.indexListContent(stepJobArgs, (i, jobArgs) -> new IndexedReference<>(jobArgs, i))
                    .map(indexedJobArgs -> createStepJobComputation(
                            containerLocation,
                            stepConfigPath,
                            args.step,
                            args.stepIndex,
                            indexedJobArgs.getPos(),
                            stepResources,
                            indexedJobArgs.getReference(),
                            jacsServiceData))
                    .collect(Collectors.toList());
            return computationFactory.newCompletedComputation(null)
                    .thenCombineAll(stepJobs, (empty, stepResults) -> new JacsServiceResult<>(jacsServiceData))
                    ;
        } catch (Exception e) {
            logger.warn("Failed to read step config for {}", jacsServiceData, e);
            return computationFactory.newFailedComputation(new ComputationException(jacsServiceData, e));
        }
    }

    private ServiceComputation<JacsServiceResult<Void>> createStepJobComputation(String containerLocation,
                                                                                Path containerBindPath,
                                                                                LightsheetPipelineStep step,
                                                                                int stepIndex,
                                                                                int jobIndex,
                                                                                Map<String, String> jobResources,
                                                                                List<String> jobArgs,
                                                                                JacsServiceData jacsServiceData) {
        return containerProcessor.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Step " + stepIndex + ": " + step + " - job " + jobIndex)
                        .addResources(jobResources)
                        .build(),
                new ServiceArg("-containerLocation", containerLocation),
                new ServiceArg("-bindPaths", containerBindPath + ":" + containerBindPath),
                new ServiceArg("-appArgs", jobArgs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(""))
        );
    }

    private Map<String, String> prepareResources(LightsheetPipelineStepArgs args, Map<String, String> jobResources) {
        String cpuType = ProcessorHelper.getCPUType(jobResources);
        if (StringUtils.isBlank(cpuType)) {
            ProcessorHelper.setCPUType(jobResources, "broadwell");
        }
        ProcessorHelper.setRequiredSlots(jobResources, args.step.getRecommendedSlots());
        ProcessorHelper.setSoftJobDurationLimitInSeconds(jobResources, 5*60); // 5 minutes
        ProcessorHelper.setHardJobDurationLimitInSeconds(jobResources, 12*60*60); // 12 hours
        return jobResources;
    }

    private LightsheetPipelineStepArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LightsheetPipelineStepArgs());
    }

    private String getContainerLocation(Map<String, Object> stepConfig, LightsheetPipelineStepArgs args) {
        String containerImage = args.containerImage;
        if (StringUtils.isNotBlank(containerImage)) {
            return containerImage;
        } else {
            containerImage = (String) stepConfig.get("containerImage " + "." + args.step + "." + args.stepIndex);
            if (containerImage == null) {
                containerImage = (String) stepConfig.get("containerImage " + "." + args.step);
            }
            if (StringUtils.isBlank(containerImage)) {
                containerImage = StringUtils.appendIfMissing(applicationConfig.getStringPropertyValue("ImageProcessing.Collection"), "/");
                containerImage += args.step.toString().toLowerCase();
                String containerImageVersion = applicationConfig.getStringPropertyValue(
                        "ImageProcessing.Lightsheet." + args.step + ".Version",
                        applicationConfig.getStringPropertyValue("ImageProcessing.Lightsheet.Version"));
                if (StringUtils.isNotBlank(containerImageVersion)) {
                    containerImage += ":" + containerImageVersion;
                }
            }
            return containerImage;
        }
    }

    private String extractStepFromConfigUrl(String configUrl) {
        String[] parts = configUrl.split("\\?stepName=");
        return parts.length > 1 ? parts[1] : null;
    }

    private Map<String, Object> readJsonConfig(InputStream inputStream) {
        try {
            return objectMapper.readValue(inputStream, new TypeReference<Map<String, Object>>() {});
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeJsonConfig(Map<String, Object> config, File configFile) {
        try {
            Files.createDirectories(configFile.getParentFile().toPath());
            objectMapper.writeValue(configFile, config);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    // Creates json file from http call
    private InputStream getJsonConfig(String configAddress, String stepName) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target;
            if ("generateMiniStacks".equals(stepName)) {
                // the address must already contain the desired step but I want to check that is present
                String stepFromConfigUrl = extractStepFromConfigUrl(configAddress);
                if (StringUtils.isBlank(stepFromConfigUrl)) {
                    throw new IllegalArgumentException("Step name missing from the config url for generateMiniStacks");
                }
                target = httpclient.target(configAddress);
            } else {
                // If the step name is not generateMiniStack then the address to the required step is provided
                target = httpclient.target(configAddress).queryParam("stepName", stepName);
            }
            Response response = target.request().get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new IllegalStateException(configAddress + " returned with " + response.getStatus());
            }
            return response.readEntity(InputStream.class);
        } catch (IllegalStateException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpclient != null) {
                httpclient.close();
            }
        }
    }

    private Pair<String, Map<String, Object>> getStepConfig(JacsServiceData jacsServiceData, LightsheetPipelineStepArgs args) {
        Map<String, Object> stepConfig = readJsonConfig(getJsonConfig(args.configAddress, args.step.name()));
        stepConfig.putAll(jacsServiceData.getActualDictionaryArgs()); // overwrite arguments that were explicitly passed by the user
        // write the final config file
        JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
        String fileName = "";
        if ("generateMiniStacks".equals(args.step.name())) { // Use previous step name for file name
            fileName = "stepConfig_" + String.valueOf(args.stepIndex) + "_" + extractStepFromConfigUrl(args.configAddress) + ".json";
        }
        else{
            fileName = "stepConfig_" + String.valueOf(args.stepIndex) + "_" + args.step.name() + ".json";
        }
        File jsonConfigFile = serviceWorkingFolder.getServiceFolder(fileName).toFile();
        writeJsonConfig(stepConfig, jsonConfigFile);
        if (StringUtils.isNotBlank(args.configOutputPath) && !"generateMiniStacks".equals(args.step.name())) {
            String[] addressParts = args.configAddress.split("/");
            String lightsheetJobID = addressParts[addressParts.length -1];
            Path configOutputPath = Paths.get(args.configOutputPath + "/" + lightsheetJobID + "/" + fileName);
            File configOutputPathFile = configOutputPath.toFile();
            writeJsonConfig(stepConfig, configOutputPathFile);
        }
        return ImmutablePair.of(jsonConfigFile.getAbsolutePath(), stepConfig);
    }


    private List<List<String>> getStepJobArgs(String jsonConfig, LightsheetPipelineStepArgs args) {
        if (args.step.cannotSplitJob()) {
            return ImmutableList.of(
                    ImmutableList.of(jsonConfig)
            );
        } else {
            int numTimePoints = args.numTimePoints;
            int timePointsPerJob;
            if (numTimePoints < 1) numTimePoints = 1;
            if (args.timePointsPerJob < 1) {
                timePointsPerJob = 1;
            } else {
                timePointsPerJob = args.timePointsPerJob;
            }
            int numJobs = (int) Math.ceil((double)numTimePoints / timePointsPerJob);
            return IntStream.range(0, numJobs)
                    .mapToObj(jobIndex -> ImmutableList.of(jsonConfig, String.valueOf(timePointsPerJob), String.valueOf(jobIndex + 1)))
                    .collect(Collectors.toList())
                    ;
        }
    }

}


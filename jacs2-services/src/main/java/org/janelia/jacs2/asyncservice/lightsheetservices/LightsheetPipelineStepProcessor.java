package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.JacsServiceFolder;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ProcessorHelper;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.containerizedservices.PullSingularityContainerProcessor;
import org.janelia.jacs2.asyncservice.containerizedservices.SimpleRunSingularityContainerProcessor;
import org.janelia.jacs2.cdi.qualifier.ApplicationProperties;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.config.ApplicationConfig;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;

/**
 * Service for running a single lightsheet pipeline step.
 *
 * @author David Ackerman
 */
@Named("lightsheetPipelineStep")
public class LightsheetPipelineStepProcessor extends AbstractServiceProcessor<Void> {

    static class LightsheetPipelineStepArgs extends ServiceArgs {
        @Parameter(names = "-step", description = "Which pipeline step to run", required = true)
        LightsheetPipelineStep step;
        @Parameter(names = "-stepIndex", description = "Which step index is this")
        Integer stepIndex = 1;
        @Parameter(names = "-containerImage", description = "Container image")
        String containerImage;
        @Parameter(names = "-numTimePoints", description = "Number of time points")
        Integer numTimePoints = 0;
        @Parameter(names = "-timePointsPerJob", description = "Number of time points per job")
        Integer timePointsPerJob = 1;
        @Parameter(names = "-configReference", description = "Job's configuration reference")
        String pipelineConfigReference;
    }

    private static class StepJobArgs {
        private final List<String> commonBatchArgs; // common arguments for all instances
        private final List<String> instanceArgs = new ArrayList<>(); // arguments specific to each member instance of the batch

        private StepJobArgs(List<String> commonBatchArgs) {
            this.commonBatchArgs = commonBatchArgs;
        }
    }

    private final WrappedServiceProcessor<PullSingularityContainerProcessor, File> pullContainerProcessor;
    private final WrappedServiceProcessor<SimpleRunSingularityContainerProcessor, Void> runContainerProcessor;
    private final ApplicationConfig applicationConfig;
    private final ObjectMapper objectMapper;

    @Inject
    LightsheetPipelineStepProcessor(ServiceComputationFactory computationFactory,
                                    JacsServiceDataPersistence jacsServiceDataPersistence,
                                    @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                    @ApplicationProperties ApplicationConfig applicationConfig,
                                    PullSingularityContainerProcessor pullContainerProcessor,
                                    SimpleRunSingularityContainerProcessor runContainerProcessor,
                                    ObjectMapper objectMapper,
                                    Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.pullContainerProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, pullContainerProcessor);
        this.runContainerProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, runContainerProcessor);
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
            Map<String, Object> stepParameters = getStepParameters(jacsServiceData.getActualDictionaryArgs());
            String stepConfigFile = getSavedStepConfigFile(jacsServiceData, args, stepParameters);
            int numTimePoints = args.numTimePoints <= 0
                    ? getNumTimePointsFromJsonConfig(stepParameters)
                    : args.numTimePoints;
            String stepConfigPath = Paths.get(stepConfigFile).getParent().toString();
            String containerLocation = getContainerLocation(stepParameters, args);
            StepJobArgs stepJobArgs = getStepJobArgs(stepConfigFile, args, numTimePoints);
            Map<String, String> stepResources = prepareResources(args, jacsServiceData.getResources());

            Map<String, String> dataMountPoints = new LinkedHashMap<>();
            dataMountPoints.put(stepConfigPath, stepConfigPath);
            dataMountPoints.putAll(getDataMountPointsFromAppConfig());
            dataMountPoints.putAll(getDataMountPointsFromDictionaryArgs(jacsServiceData.getActualDictionaryArgs()));
            dataMountPoints.putAll(getDataMountPointsFromStepConfig(stepParameters));

            return pullContainerProcessor.process(
                    new ServiceExecutionContext.Builder(jacsServiceData)
                            .description("Pull container image " + containerLocation)
                            .build(),
                    new ServiceArg("-containerLocation", containerLocation))
                    .thenCompose(containerImageResult -> createStepJobComputation(
                                containerImageResult,
                                dataMountPoints,
                                args.step,
                                args.stepIndex,
                                stepResources,
                                stepJobArgs.commonBatchArgs,
                                stepJobArgs.instanceArgs,
                                jacsServiceData))
                    .thenApply(r -> new JacsServiceResult<>(jacsServiceData))
                    ;
        } catch (Exception e) {
            logger.warn("Failed to read step config for {}", jacsServiceData, e);
            return computationFactory.newFailedComputation(new ComputationException(jacsServiceData, e));
        }
    }

    private ServiceComputation<JacsServiceResult<Void>> createStepJobComputation(JacsServiceResult<File> containerImageResult,
                                                                                 Map<String, String> mountPoints,
                                                                                 LightsheetPipelineStep step,
                                                                                 int stepIndex,
                                                                                 Map<String, String> jobResources,
                                                                                 List<String> stepArgs,
                                                                                 List<String> jobInstanceArgs,
                                                                                 JacsServiceData jacsServiceData) {
        String bindPath = mountPoints.entrySet().stream()
                .map(en -> {
                    if (StringUtils.isBlank(en.getValue())) {
                        return en.getKey();
                    } else {
                        return en.getKey() + ":" + en.getValue();
                    }
                })
                .reduce((b1, b2) -> b1 + "," + b2)
                .orElse("");
        return runContainerProcessor.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Step " + stepIndex + ": " + step +
                                (jobInstanceArgs.isEmpty() ? "" : " - " + jobInstanceArgs.size() + " timepoint jobs"))
                        .waitFor(containerImageResult.getJacsServiceData())
                        .addResources(jobResources)
                        .build(),
                new ServiceArg("-containerLocation", containerImageResult.getResult().getAbsolutePath()),
                new ServiceArg("-bindPaths", bindPath),
                new ServiceArg("-appArgs", stepArgs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse("")),
                new ServiceArg("-batchJobArgs", jobInstanceArgs.stream().reduce((s1, s2) -> s1 + "," + s2).orElse(""))
        );
    }

    private LightsheetPipelineStepArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LightsheetPipelineStepArgs());
    }

    private String getContainerLocation(Map<String, Object> stepConfig, LightsheetPipelineStepArgs args) {
        String containerImage = args.containerImage;
        if (StringUtils.isNotBlank(containerImage)) {
            return containerImage;
        } else {
            containerImage = (String) stepConfig.get("containerImage");
            if (StringUtils.isBlank(containerImage)) {
                containerImage = StringUtils.appendIfMissing(applicationConfig.getStringPropertyValue("ImageProcessing.Collection"), "/");
                containerImage += args.step.toString().toLowerCase();
                String containerImageVersion = applicationConfig.getStringPropertyValue(
                        "ImageProcessing.Lightsheet." + args.step + ".Version",
                        applicationConfig.getStringPropertyValue("ImageProcessing.Lightsheet.Version"));
                if (StringUtils.isNotBlank(containerImageVersion)) {
                    containerImage += containerImageVersion;
                }
                String containerImageExt = applicationConfig.getStringPropertyValue("Singularity.Image.DefaultExt");
                if (StringUtils.isNotBlank(containerImageExt)) {
                    containerImage += containerImageExt;
                }
            }
            return containerImage;
        }
    }

    private Map<String, String> prepareResources(LightsheetPipelineStepArgs args, Map<String, String> jobResources) {
        ProcessorHelper.setRequiredSlots(jobResources, args.step.getRecommendedSlots());
        ProcessorHelper.setSoftJobDurationLimitInSeconds(jobResources, 5*60); // 5 minutes
        ProcessorHelper.setHardJobDurationLimitInSeconds(jobResources, 12*60*60); // 12 hours
        return jobResources;
    }

    private Map<String, String> getDataMountPointsFromDictionaryArgs(Map<String, Object> dictionaryArgs) {
        String dataMountPoints =  (String) dictionaryArgs.get("dataMountPoints");
        return parseMountPoints(dataMountPoints);
    }

    private Map<String, String> getDataMountPointsFromAppConfig() {
        return parseMountPoints(applicationConfig.getStringPropertyValue("ImageProcessing.Lightsheet.DataMountPoints"));
    }

    private Map<String, String> getDataMountPointsFromStepConfig(Map<String, Object> stepConfig) {
        Map<String, String> dataMountPoints = new LinkedHashMap<>();
        Function<String, Optional<Pair<String, String>>> idMapping = (String i) -> Optional.of(ImmutablePair.of(i, i));
        Function<String, Optional<Pair<String, String>>> existingPathOrParentMapping = (String ip) -> {
            Path p = Paths.get(ip);
            while (p != null && !p.toFile().exists()) p = p.getParent();
            if (p == null) {
                return Optional.empty();
            } else {
                String op = p.toString();
                return Optional.of(ImmutablePair.of(op, op));
            }
        };
        Function<String, String> parentPath = (String ip) -> Paths.get(ip).getParent().toString();

        // clusterPT
        dataMountPoints.putAll(addMountPointFromStepConfig("inputFolder", stepConfig,
                parentPath.andThen(existingPathOrParentMapping)));
	    dataMountPoints.putAll(addMountPointFromStepConfig("outputFolder", stepConfig,
                existingPathOrParentMapping));
        // clusterMF
        dataMountPoints.putAll(addMountPointFromStepConfig("inputString", stepConfig,
                idMapping));
        dataMountPoints.putAll(addMountPointFromStepConfig("outputString", stepConfig,
                parentPath.andThen(existingPathOrParentMapping)));
        // localAP
        dataMountPoints.putAll(addMountPointFromStepConfig("configRoot", stepConfig,
                existingPathOrParentMapping));
        // clusterTF
        dataMountPoints.putAll(addMountPointFromStepConfig("sourceString", stepConfig,
                parentPath.andThen(existingPathOrParentMapping)));
        dataMountPoints.putAll(addMountPointFromStepConfig("lookUpTable", stepConfig,
                parentPath.andThen(existingPathOrParentMapping)));
        // localEC
        dataMountPoints.putAll(addMountPointFromStepConfig("inputRoot", stepConfig,
                parentPath.andThen(existingPathOrParentMapping)));
        // clusterCS
        dataMountPoints.putAll(addMountPointFromStepConfig("outputRoot", stepConfig,
                existingPathOrParentMapping));
        // clusterFR
        dataMountPoints.putAll(addMountPointFromStepConfig("inputDir", stepConfig,
                idMapping));
        dataMountPoints.putAll(addMountPointFromStepConfig("outputDir", stepConfig,
                existingPathOrParentMapping));
        return dataMountPoints;
    }

    private Map<String, String> parseMountPoints(String mountPointsString) {
        if (StringUtils.isBlank(mountPointsString)) {
            return ImmutableMap.of();
        } else {
            return Splitter.on(',').trimResults().omitEmptyStrings().splitToList(mountPointsString).stream()
                    .map(kv -> {
                        int kvSepIndex = kv.indexOf(':');
                        if (kvSepIndex > 0) {
                            return ImmutablePair.of(kv.substring(0, kvSepIndex), kv.substring(kvSepIndex + 1));
                        } else if (kvSepIndex == 0) {
                            return ImmutablePair.of(kv.substring(1), kv.substring(1));
                        } else {
                            return ImmutablePair.of(kv, "");
                        }
                    })
                    .collect(LinkedHashMap::new, (m, p) -> m.put(p.getLeft(), p.getRight()), Map::putAll);
        }
    }

    private Map<String, String> addMountPointFromStepConfig(String pathKey,
                                                            Map<String, Object> stepConfig,
                                                            Function<String, Optional<Pair<String, String>>> mapper) {
        String pathValue = (String) stepConfig.get(pathKey);
        Optional<Pair<String, String>> mountPoint;
        if (StringUtils.isNotBlank(pathValue)) {
            mountPoint = mapper.apply(pathValue);
        } else {
            mountPoint = Optional.empty();
        }
        return mountPoint.map(mp -> ImmutableMap.of(mp.getLeft(), mp.getRight()))
                .orElse(ImmutableMap.of());
    }

    private String getSavedStepConfigFile(JacsServiceData jacsServiceData, LightsheetPipelineStepArgs args, Map<String, Object> stepConfig) {
        // write the final config file
        JacsServiceFolder serviceWorkingFolder = getWorkingDirectory(jacsServiceData);
        String fileName = "stepConfig_" + (StringUtils.isNotBlank(args.pipelineConfigReference) ? args.pipelineConfigReference + "_" : "") +
                String.valueOf(args.stepIndex) + "_" + args.step.name() + ".json";
        File jsonConfigFile = serviceWorkingFolder.getServiceFolder(fileName).toFile();
        writeJsonConfig(stepConfig, jsonConfigFile);
        return jsonConfigFile.getAbsolutePath();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getStepParameters(Map<String, Object> stepServiceArgs) {
        Map<String, Object> parameters = (Map<String, Object>) stepServiceArgs.get("parameters");
        return parameters == null ? Collections.emptyMap() : parameters;
    }

    @SuppressWarnings("unchecked")
    private int getNumTimePointsFromJsonConfig(Map<String, Object> config) {
        Map<String, Number> timePoints = objectMapper.convertValue(config.get("timepoints"), Map.class);
        if (timePoints == null || !timePoints.keySet().containsAll(ImmutableSet.of("start", "end", "every"))) {
            return 1;
        } else {
            return (int)(Math.ceil((timePoints.get("end").doubleValue() - timePoints.get("start").doubleValue()) / timePoints.get("every").doubleValue()) + 1);
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

    private StepJobArgs getStepJobArgs(String jsonConfig, LightsheetPipelineStepArgs args, int numTimePointsArg) {
        if (args.step.cannotSplitJob()) {
            return new StepJobArgs(ImmutableList.of(jsonConfig));
        } else {
            int numTimePoints;
            if (numTimePointsArg < 1) {
                numTimePoints = 1;
            } else {
                numTimePoints = numTimePointsArg;
            }
            int timePointsPerJob;
            if (args.timePointsPerJob < 1) {
                timePointsPerJob = 1;
            } else {
                timePointsPerJob = args.timePointsPerJob;
            }
            int numJobs = (int) Math.ceil((double)numTimePoints / timePointsPerJob);
            StepJobArgs stepJobArgs = new StepJobArgs(ImmutableList.of(jsonConfig, String.valueOf(timePointsPerJob)));
            IntStream.range(0, numJobs)
                    .forEach(jobIndex -> stepJobArgs.instanceArgs.add(String.valueOf(jobIndex + 1)))
                    ;
            return stepJobArgs;
        }
    }

}


package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.*;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import org.janelia.jacs2.utils.HttpUtils;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

/**
 * Complete lightsheet processing service which invokes multiple LightsheetPipelineProcessor steps.
 *
 * @author David Ackerman
 */
@Named("lightsheetProcessing")
public class LightsheetPipelineProcessor extends AbstractServiceProcessor<Void> {
    private static final String DEFAULT_CONFIG_OUTPUT_PATH = "";
    private static final List<String> clusterSteps = Arrays.asList("clusterPT", "clusterMF", "clusterTF", "clusterCS", "clusterFR");

    static class LightsheetProcessingArgs extends ServiceArgs {
        @Parameter(names = "-configAddress", description = "Address for accessing job's config json.", required=true)
        String configAddress;
    }

    private final WrappedServiceProcessor<LightsheetPipelineStepProcessor, Void> lightsheetPipelineStepProcessor;
    private final ObjectMapper objectMapper;

    @Inject
    LightsheetPipelineProcessor(ServiceComputationFactory computationFactory,
                                JacsServiceDataPersistence jacsServiceDataPersistence,
                                @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                LightsheetPipelineStepProcessor lightsheetPipelineStepProcessor,
                                ObjectMapper objectMapper,
                                Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.lightsheetPipelineStepProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, lightsheetPipelineStepProcessor);
        this.objectMapper = objectMapper;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LightsheetPipelineProcessor.class, new LightsheetProcessingArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        LightsheetProcessingArgs args = getArgs(jacsServiceData);
        final Integer timePointsPerJob = 1; // FIXED AT 4
        Map<String, String> argumentsToRunJob = readJsonConfig(getJsonConfig(args.configAddress));
        String[] currentJobStepNames =  argumentsToRunJob.get("currentJACSJobStepNames").split(",");
        String[] currentJobTimePoints =  argumentsToRunJob.get("currentJACSJobTimePoints").split(",");
        String configOutputPath =  argumentsToRunJob.get("configOutputPath");
        ServiceComputation<JacsServiceResult<Void>> stage = lightsheetPipelineStepProcessor.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Step 0: " + currentJobStepNames[0])
                        .addDictionaryArgs((Map<String, Object>) jacsServiceData.getDictionaryArgs().get(currentJobStepNames[0]))
                        .build(),
                new ServiceArg("-step", currentJobStepNames[0]),
                new ServiceArg("-stepIndex", 0),
                new ServiceArg("-containerImage", getStepContainerImage(argumentsToRunJob, currentJobStepNames[0], 0)),
                new ServiceArg("-numTimePoints",currentJobTimePoints[0]),
                new ServiceArg("-timePointsPerJob", timePointsPerJob.toString()),
                new ServiceArg("-configAddress", args.configAddress),
                new ServiceArg("-configOutputPath", configOutputPath)
        );
        int nSteps = currentJobStepNames.length;
        for (int i = 1; i < nSteps; i++) {
            final int stepIndex=i;
            stage = stage.thenCompose(previousStageResult -> lightsheetPipelineStepProcessor.process(
                        new ServiceExecutionContext.Builder(jacsServiceData)
                                .description("Step " + String.valueOf(stepIndex) + ": " +currentJobStepNames[stepIndex])
                                .waitFor(previousStageResult.getJacsServiceData()) // for dependency based on previous step
                                .build(),
                        new ServiceArg("-step", currentJobStepNames[stepIndex]),
                        new ServiceArg("-stepIndex", stepIndex),
                        new ServiceArg("-containerImage", getStepContainerImage(argumentsToRunJob, currentJobStepNames[stepIndex], stepIndex)),
                        new ServiceArg("-numTimePoints", currentJobTimePoints[stepIndex]),
                        new ServiceArg("-timePointsPerJob", timePointsPerJob.toString()),
                        new ServiceArg("-configAddress", args.configAddress),
                        new ServiceArg("-configOutputPath", configOutputPath)
                ))
            ;
        }
        return stage.thenApply((JacsServiceResult<Void> lastStepResult) -> new JacsServiceResult<>(jacsServiceData, lastStepResult.getResult()));
    }

    private LightsheetProcessingArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LightsheetProcessingArgs());
    }

    private Map<String, String> readJsonConfig(InputStream inputStream) {
        try {
            return objectMapper.readValue(inputStream, new TypeReference<Map<String, String>>() {});
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private InputStream getJsonConfig(String configAddress) {
        Client httpclient = null;
        try {
            httpclient = HttpUtils.createHttpClient();
            WebTarget target;
            // If the step name is not generateMiniStack then the address to the required step is provided
            target = httpclient.target(configAddress).queryParam("stepName", "getArgumentsToRunJob");
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

    private String getStepContainerImage(Map<String, String> pipelineArgs, String stepName, Integer stepIndex) {
        String containerImage = pipelineArgs.get("containerImage " + "." + stepName + "." + stepIndex);
        if (containerImage == null) {
            containerImage = pipelineArgs.get("containerImage " + "." + stepName);
        }
        return StringUtils.defaultIfBlank(containerImage, "");
    }
}

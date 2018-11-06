package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.utils.HttpUtils;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;

/**
 * Complete lightsheet pipeline processing service which invokes multiple LightsheetPipelineProcessor steps.
 *
 * @author David Ackerman
 */
@Named("lightsheetPipeline")
public class LightsheetPipelineProcessor extends AbstractServiceProcessor<Void> {

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
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        LightsheetProcessingArgs args = getArgs(jacsServiceData);
        Map<String, String> argumentsToRunJob = readJsonConfig(getJsonConfig(args.configAddress));
        String currentJACSJobStepNameValues = argumentsToRunJob.get("currentJACSJobStepNames");
        Preconditions.checkArgument(StringUtils.isNotBlank(currentJACSJobStepNameValues),
                "currentJACSJobStepNames is not set");
        String[] currentJobStepNames =  currentJACSJobStepNameValues.split(",");
        String configOutputPath =  argumentsToRunJob.get("configOutputPath");
        ServiceComputation<JacsServiceResult<Void>> stage = lightsheetPipelineStepProcessor.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .description("Step 1: " + currentJobStepNames[0])
                        .addDictionaryArgs(getStepDictionaryArgs(jacsServiceData.getDictionaryArgs(), currentJobStepNames[0]))
                        .addDictionaryArgs(getStepDictionaryArgs(jacsServiceData.getDictionaryArgs(), currentJobStepNames[0] + ".0"))
                        .build(),
                new ServiceArg("-step", currentJobStepNames[0]),
                new ServiceArg("-stepIndex", 0),
                new ServiceArg("-configAddress", args.configAddress),
                new ServiceArg("-configOutputPath", configOutputPath)
        );
        int nSteps = currentJobStepNames.length;
        for (int i = 1; i < nSteps; i++) {
            final int stepIndex=i;
            stage = stage.thenCompose(previousStageResult -> lightsheetPipelineStepProcessor.process(
                    new ServiceExecutionContext.Builder(jacsServiceData)
                            .description("Step " + String.valueOf(stepIndex) + ": " + currentJobStepNames[stepIndex])
                            .addDictionaryArgs(getStepDictionaryArgs(jacsServiceData.getDictionaryArgs(), currentJobStepNames[stepIndex]))
                            .addDictionaryArgs(getStepDictionaryArgs(jacsServiceData.getDictionaryArgs(), currentJobStepNames[stepIndex] + "." + stepIndex))
                            .waitFor(previousStageResult.getJacsServiceData()) // for dependency based on previous step
                            .build(),
                    new ServiceArg("-step", currentJobStepNames[stepIndex]),
                    new ServiceArg("-stepIndex", stepIndex),
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
                logger.error("Request for json config to {} returned with {}", target, response.getStatus());
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

    @SuppressWarnings("unchecked")
    private Map<String, Object> getStepDictionaryArgs(Map<String, Object> dictionaryArgs, String stepKey) {
         Object stepConfig = dictionaryArgs.get(stepKey);
         if (stepConfig instanceof Map) {
             return (Map<String, Object>) stepConfig;
         } else {
             return ImmutableMap.of();
         }
    }

}

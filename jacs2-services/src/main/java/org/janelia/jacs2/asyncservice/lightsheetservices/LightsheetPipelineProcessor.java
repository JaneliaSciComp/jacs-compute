package org.janelia.jacs2.asyncservice.lightsheetservices;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
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
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Complete lightsheet pipeline processing service which invokes multiple LightsheetPipelineProcessor steps.
 *
 * @author David Ackerman
 */
@Named("lightsheetPipeline")
public class LightsheetPipelineProcessor extends AbstractServiceProcessor<Void> {

    static class LightsheetProcessingArgs extends ServiceArgs {
        @Parameter(names = "-configAddress", description = "Address for accessing job's config json.")
        String pipelineConfigURL;
        @Parameter(names = "-configReference", description = "Job's configuration reference")
        String pipelineConfigReference;

        LightsheetProcessingArgs() {
            super("Lightsheet processor. This is a processor that can pipeline multiple lightsheet steps");
        }
    }

    private final WrappedServiceProcessor<LightsheetPipelineStepProcessor, Void> lightsheetPipelineStepProcessor;

    @Inject
    LightsheetPipelineProcessor(ServiceComputationFactory computationFactory,
                                JacsServiceDataPersistence jacsServiceDataPersistence,
                                @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                LightsheetPipelineStepProcessor lightsheetPipelineStepProcessor,
                                Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.lightsheetPipelineStepProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, lightsheetPipelineStepProcessor);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(LightsheetPipelineProcessor.class, new LightsheetProcessingArgs());
    }

    @SuppressWarnings("unchecked")
    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        LightsheetProcessingArgs args = getArgs(jacsServiceData);
        Map<String, Object> serviceArgs = jacsServiceData.getActualDictionaryArgs();
        Map<String, Object> lightsheetPipelineConfig = serviceArgs.containsKey("pipelineConfig")
                ? (Map<String, Object>) serviceArgs.get("pipelineConfig")
                : new LinkedHashMap<>();
        if (lightsheetPipelineConfig.isEmpty() && StringUtils.isNotBlank(args.pipelineConfigURL)) {
            lightsheetPipelineConfig.putAll(getJsonConfig(args.pipelineConfigURL));
        }
        List<Map<String, Object>> lightsheetStepsConfigs = getLightsheetSteps(lightsheetPipelineConfig);

        ServiceComputation<JacsServiceResult<Void>> stage = computationFactory.newCompletedComputation(new JacsServiceResult<>(jacsServiceData));
        int index = 0;
        for (Map<String, Object> lightsheetStepConfig : lightsheetStepsConfigs) {
            String stepName = getStepName(lightsheetStepConfig);
            final int stepIndex=index;
            String description = String.format("Step %d - running %s", stepIndex + 1, stepName);
            stage = stage.thenCompose(previousStageResult -> lightsheetPipelineStepProcessor.process(
                    new ServiceExecutionContext.Builder(jacsServiceData)
                            .description(description)
                            .waitFor(previousStageResult.getJacsServiceData())
                            .addDictionaryArgs(lightsheetStepConfig)
                            .addResources(jacsServiceData.getResources())
                            .addResources(getStepResources(lightsheetStepConfig))
                            .build(),
                    new ServiceArg("-step", stepName),
                    new ServiceArg("-stepIndex", stepIndex),
                    new ServiceArg("-configReference", args.pipelineConfigReference)
            ));
            index++;
        }
        // wait until all steps are done and then return the last result
        return stage.thenSuspendUntil((JacsServiceResult<Void> lastStepResult) -> new ContinuationCond.Cond<>(lastStepResult, areAllDependenciesDone(jacsServiceData)))
                .thenApply((JacsServiceResult<Void> lastStepResult) -> new JacsServiceResult<>(jacsServiceData, lastStepResult.getResult()));
    }

    private LightsheetProcessingArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LightsheetProcessingArgs());
    }

    private Map<String, Object> getJsonConfig(String configURL) {
        Client httpclient = HttpUtils.createHttpClient();
        try {
            WebTarget target = httpclient.target(configURL);
            Response response = target.request().get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                logger.error("Request for json config to {} returned with {}", target, response.getStatus());
                throw new IllegalStateException(configURL + " returned with " + response.getStatus());
            }
            List<Map<String, Object>> configs = response.readEntity(new GenericType<>(new TypeReference<List<Map<String, Object>>>(){}.getType()));
            if (CollectionUtils.isNotEmpty(configs)) {
                return configs.get(0);
            } else {
                return ImmutableMap.of();
            }
        } catch (IllegalStateException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpclient.close();
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getLightsheetSteps(Map<String, Object> lightsheetPipelineConfig) {
        List<Map<String, Object>> lightsheetSteps = (List<Map<String, Object>>) lightsheetPipelineConfig.get("steps");
        return lightsheetSteps == null ? Collections.emptyList() : lightsheetSteps;
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> getStepResources(Map<String, Object> stepConfig) {
        Map<String, String> stepResources = (Map<String, String>)stepConfig.get("stepResources");
        return stepResources != null ? stepResources : Collections.emptyMap();
    }

    private String getStepName(Map<String, Object> stepConfig) {
        return (String)stepConfig.get("name");
    }
}

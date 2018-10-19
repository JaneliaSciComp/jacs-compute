package org.janelia.jacs2.asyncservice.pipeline;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.GenericAsyncServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Pipeline service. The services expects a dictionary argument
 */
@Named("pipeline")
public class PipelineServiceProcessor extends AbstractServiceProcessor<Void> {

    static class PipelineProcessingArgs extends ServiceArgs {
        @Parameter(names = "-configURL", description = "Configuration URL")
        String pipelineConfigURL;

        PipelineProcessingArgs() {
            super("Pipeline processor. This is a processor that can pipeline multiple services");
        }
    }

    private final GenericAsyncServiceProcessor genericAsyncServiceProcessor;

    @Inject
    PipelineServiceProcessor(ServiceComputationFactory computationFactory,
                             JacsServiceDataPersistence jacsServiceDataPersistence,
                             @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                             GenericAsyncServiceProcessor genericAsyncServiceProcessor,
                             Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.genericAsyncServiceProcessor = genericAsyncServiceProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(PipelineServiceProcessor.class, new PipelineProcessingArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @SuppressWarnings("unchecked")
    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        PipelineProcessingArgs args = getArgs(jacsServiceData);
        Map<String, Object> serviceArgs = jacsServiceData.getActualDictionaryArgs();
        Map<String, Object> pipelineConfig = serviceArgs.containsKey("pipelineConfig")
                ? (Map<String, Object>) serviceArgs.get("pipelineConfig")
                : new LinkedHashMap<>();
        if (StringUtils.isNotBlank(args.pipelineConfigURL)) {
            // the configuration specified in the URL overrides the one given to the service
            pipelineConfig.putAll(getJsonConfig(args.pipelineConfigURL));
        }
        List<Map<String, Object>> servicesConfigs = getPipelineServices(pipelineConfig);
        List<String> runningSteps = getRunningSteps(pipelineConfig);
        Predicate<String> shouldIdRunStep = stepName -> runningSteps == null || runningSteps.contains(stepName);

        ServiceComputation<JacsServiceResult<Void>> stage = computationFactory.newCompletedComputation(new JacsServiceResult<>(null));
        int index = 1;
        for (Map<String, Object> serviceConfig : servicesConfigs) {
            String stepServiceName = getServiceName(serviceConfig);
            if (shouldIdRunStep.test(stepServiceName)) {
                int stepIndex = index;
                stage.thenCompose(previousStageResult -> genericAsyncServiceProcessor.process(
                        new ServiceExecutionContext.Builder(jacsServiceData)
                                .description("Step " + stepIndex + ":" + stepServiceName)
                                .waitFor(previousStageResult.getJacsServiceData())
                                .addResources(jacsServiceData.getResources())
                                .addResources(getServiceResources(serviceConfig))
                                .addDictionaryArgs(getServiceDictionaryArgs(serviceConfig))
                                .build(),
                        new ServiceArg("-serviceName", stepServiceName),
                        new ServiceArg("-serviceArgs", getServiceArgs(serviceConfig))
                ));
                index++;
            }
        }
        return stage.thenApply((JacsServiceResult<Void> lastStepResult) -> new JacsServiceResult<>(jacsServiceData, lastStepResult.getResult()));
    }

    private PipelineProcessingArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new PipelineProcessingArgs());
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
            return response.readEntity(new GenericType<>(new TypeReference<Map<String, Object>>(){}.getType()));
        } catch (IllegalStateException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            httpclient.close();
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getPipelineServices(Map<String, Object> pipelineConfig) {
        List<Map<String, Object>> pipelineServices = (List<Map<String, Object>>) pipelineConfig.get("pipelineServices");
        return pipelineServices == null ? ImmutableList.of() : pipelineServices;
    }

    @SuppressWarnings("unchecked")
    private List<String> getRunningSteps(Map<String, Object> pipelineConfig) {
        return (List<String>) pipelineConfig.get("runningSteps");
    }

    private String getServiceName(Map<String, Object> serviceConfig) {
        return (String)serviceConfig.get("serviceName");
    }

    @SuppressWarnings("unchecked")
    private List<String> getServiceArgs(Map<String, Object> serviceConfig) {
        return (List<String>)serviceConfig.get("serviceArgs");
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> getServiceResources(Map<String, Object> serviceConfig) {
        return (Map<String, String>)serviceConfig.get("serviceResources");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getServiceDictionaryArgs(Map<String, Object> serviceConfig) {
        return (Map<String, Object>)serviceConfig.get("serviceKeyArgs");
    }

}

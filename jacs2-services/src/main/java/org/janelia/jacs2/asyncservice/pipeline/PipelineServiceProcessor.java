package org.janelia.jacs2.asyncservice.pipeline;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
import org.janelia.jacs2.asyncservice.common.GenericAsyncServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.utils.HttpUtils;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ProcessingLocation;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
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
        BiPredicate<String, String> shouldIdRunStep = (stepName, serviceName) -> {
            if (runningSteps == null) {
                return true;
            } else if (StringUtils.isNotBlank(stepName)) {
                return runningSteps.stream()
                        .filter(Predicate.isEqual(stepName).or(Predicate.isEqual(serviceName)))
                        .findFirst()
                        .map(s -> true).orElse(false);
            } else {
                return runningSteps.stream()
                        .filter(Predicate.isEqual(serviceName))
                        .findFirst()
                        .map(s -> true).orElse(false);
            }
        };
        ServiceComputation<JacsServiceResult<Void>> stage = computationFactory.newCompletedComputation(new JacsServiceResult<>(null));
        int index = 1;
        for (Map<String, Object> serviceConfig : servicesConfigs) {
            String stepServiceName = getServiceName(serviceConfig);
            String stepName = getStepName(serviceConfig);
            if (shouldIdRunStep.test(stepName, stepServiceName)) {
                int stepIndex = index;
                String description;
                if (StringUtils.isNotBlank(stepName)) {
                    description = String.format("Step %d: %s - running %s", stepIndex, stepName, stepServiceName);
                } else {
                    description = String.format("Step %d - running %s", stepIndex, stepServiceName);
                }
                stage = stage.thenCompose(previousStageResult -> genericAsyncServiceProcessor.process(
                        new ServiceExecutionContext.Builder(jacsServiceData)
                                .description(description)
                                .waitFor(previousStageResult.getJacsServiceData())
                                .processingLocation(getServiceProcessingLocation(serviceConfig))
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
        // wait until all steps are done and then return the last result
        return stage.thenSuspendUntil((JacsServiceResult<Void> lastStepResult) -> new ContinuationCond.Cond<>(lastStepResult, areAllDependenciesDone(jacsServiceData)))
                .thenApply((JacsServiceResult<Void> lastStepResult) -> new JacsServiceResult<>(jacsServiceData, lastStepResult.getResult()));
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
        return pipelineServices == null ? Arrays.asList() : pipelineServices;
    }

    @SuppressWarnings("unchecked")
    private List<String> getRunningSteps(Map<String, Object> pipelineConfig) {
        return (List<String>) pipelineConfig.get("runningSteps");
    }

    private String getServiceName(Map<String, Object> serviceConfig) {
        return (String)serviceConfig.get("serviceName");
    }

    private String getStepName(Map<String, Object> serviceConfig) {
        return (String)serviceConfig.get("stepName");
    }

    @SuppressWarnings("unchecked")
    private List<String> getServiceArgs(Map<String, Object> serviceConfig) {
        List<String> serviceArgs = (List<String>)serviceConfig.get("serviceListArgs");
        return serviceArgs != null ? serviceArgs : Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> getServiceResources(Map<String, Object> serviceConfig) {
        Map<String, String> serviceResources = (Map<String, String>)serviceConfig.get("serviceResources");
        return serviceResources != null ? serviceResources : Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getServiceDictionaryArgs(Map<String, Object> serviceConfig) {
        Map<String, Object> serviceKeyArgs = (Map<String, Object>)serviceConfig.get("serviceKeyArgs");
        return serviceKeyArgs != null ? serviceKeyArgs : Collections.emptyMap();
    }

    private ProcessingLocation getServiceProcessingLocation(Map<String, Object> serviceConfig) {
        String serviceProcessingLocation = (String) serviceConfig.get("serviceProcessingLocation");
        if (StringUtils.isNotBlank(serviceProcessingLocation)) {
            return ProcessingLocation.valueOf(serviceProcessingLocation);
        } else {
            return null;
        }
    }
}

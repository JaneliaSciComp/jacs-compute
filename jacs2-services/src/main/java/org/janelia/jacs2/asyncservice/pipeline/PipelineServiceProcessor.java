package org.janelia.jacs2.asyncservice.pipeline;

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
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ProcessingLocation;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
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
        Map<String, Object> serviceArgs = jacsServiceData.getActualDictionaryArgs();
        Map<String, Object> pipelineConfig = serviceArgs.containsKey("pipelineConfig")
                ? (Map<String, Object>) serviceArgs.get("pipelineConfig")
                : new LinkedHashMap<>();
        List<Map<String, Object>> servicesConfigs = getPipelineServices(pipelineConfig);
        List<String> runningSteps = getRunningSteps(pipelineConfig);
        BiPredicate<String, String> shouldItRunStep = (stepName, serviceName) -> {
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
            if (shouldItRunStep.test(stepName, stepServiceName)) {
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
                                .setServiceTimeoutInMillis(getServiceTimeout(serviceConfig))
                                .addDictionaryArgs(getServiceDictionaryArgs(serviceConfig))
                                .addResources(jacsServiceData.getResources())
                                .addResources(getServiceResources(serviceConfig))
                                .build(),
                        new ServiceArg("-serviceName", stepServiceName),
                        new ServiceArg(getServiceArgs(serviceConfig)) // pass in service args exactly as they are
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

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getPipelineServices(Map<String, Object> pipelineConfig) {
        List<Map<String, Object>> pipelineServices = (List<Map<String, Object>>) pipelineConfig.get("pipelineServices");
        return pipelineServices == null ? Collections.emptyList() : pipelineServices;
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
        List<String> serviceArgs = (List<String>)serviceConfig.get("serviceArgs");
        return serviceArgs != null ? serviceArgs : Collections.emptyList();
    }

    private Long getServiceTimeout(Map<String, Object> serviceConfig) {
        String serviceTimeout = (String) serviceConfig.get("serviceTimeout");
        return StringUtils.isBlank(serviceTimeout) ? null : Long.valueOf(serviceTimeout.trim());
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

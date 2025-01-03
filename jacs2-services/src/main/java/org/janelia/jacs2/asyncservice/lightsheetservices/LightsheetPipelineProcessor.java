package org.janelia.jacs2.asyncservice.lightsheetservices;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.google.common.collect.ImmutableSet;
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
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

/**
 * Complete lightsheet pipeline processing service which invokes multiple LightsheetPipelineProcessor steps.
 *
 * @author David Ackerman
 */
@Named("lightsheetPipeline")
public class LightsheetPipelineProcessor extends AbstractServiceProcessor<Void> {

    private static final Set<String> NON_EXECUTABLE_STEPS = ImmutableSet.of("globalParameters");

    static class LightsheetProcessingArgs extends ServiceArgs {
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
        List<Map<String, Object>> lightsheetStepsConfigs = getLightsheetSteps(lightsheetPipelineConfig);
        ServiceComputation<JacsServiceResult<Void>> stage = computationFactory.newCompletedComputation(new JacsServiceResult<>(null));
        int index = 0;
        for (Map<String, Object> lightsheetStepConfig : lightsheetStepsConfigs) {
            String stepName = getStepName(lightsheetStepConfig);
            if (!NON_EXECUTABLE_STEPS.contains(stepName)) {
                final int stepIndex = index;
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
                        new ServiceArg("-stepIndex", stepIndex)
                ));
                index++;
            }
        }
        // wait until all steps are done and then return the last result
        return stage.thenSuspendUntil((JacsServiceResult<Void> lastStepResult) -> new ContinuationCond.Cond<>(lastStepResult, areAllDependenciesDone(jacsServiceData)))
                .thenApply((JacsServiceResult<Void> lastStepResult) -> new JacsServiceResult<>(jacsServiceData, lastStepResult.getResult()));
    }

    private LightsheetProcessingArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LightsheetProcessingArgs());
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

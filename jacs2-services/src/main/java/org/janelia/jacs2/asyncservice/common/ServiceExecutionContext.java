package org.janelia.jacs2.asyncservice.common;

import com.google.common.base.Preconditions;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ProcessingLocation;
import org.janelia.model.service.RegisteredJacsNotification;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ServiceExecutionContext {

    public static class Builder {
        private final ServiceExecutionContext serviceExecutionContext;

        public Builder(JacsServiceData parentServiceData) {
            Preconditions.checkArgument(parentServiceData != null);
            serviceExecutionContext = new ServiceExecutionContext(parentServiceData);
        }

        public Builder processingLocation(ProcessingLocation processingLocation) {
            serviceExecutionContext.processingLocation = processingLocation;
            return this;
        }

        public Builder waitFor(List<JacsServiceData> dependenciesList) {
            for (JacsServiceData dependency : dependenciesList) {
                if (dependency != null) serviceExecutionContext.waitFor.add(dependency);
            }
            return this;
        }

        public Builder waitFor(JacsServiceData... dependencies) {
            for (JacsServiceData dependency : dependencies) {
                if (dependency != null) serviceExecutionContext.waitFor.add(dependency);
            }
            return this;
        }

        public Builder waitFor(Number... dependenciesIds) {
            for (Number dependencyId : dependenciesIds) {
                if (dependencyId != null) serviceExecutionContext.waitForIds.add(dependencyId);
            }
            return this;
        }

        public Builder state(JacsServiceState state) {
            serviceExecutionContext.serviceState = state;
            return this;
        }

        public Builder setServiceName(String serviceName) {
            serviceExecutionContext.serviceName = serviceName;
            return this;
        }

        public Builder setOutputPath(String outputPath) {
            serviceExecutionContext.outputPath = outputPath;
            return this;
        }

        public Builder setErrorPath(String errorPath) {
            serviceExecutionContext.errorPath = errorPath;
            return this;
        }

        public Builder setWorkspace(String workspace) {
            serviceExecutionContext.workspace = workspace;
            return this;
        }

        public Builder description(String description) {
            serviceExecutionContext.description = description;
            return this;
        }

        public Builder addRequiredMemoryInGB(int mem) {
            ProcessorHelper.setRequiredMemoryInGB(serviceExecutionContext.resources, mem);
            return this;
        }

        public Builder addResources(Map<String, String> srcResources) {
            serviceExecutionContext.resources.putAll(srcResources);
            return this;
        }

        public Builder registerProcessingNotification(String processingEvent, Optional<RegisteredJacsNotification> processingNotification) {
            processingNotification.ifPresent(n -> serviceExecutionContext.processingNotification = n.withEventName(processingEvent));
            return this;
        }

        public Builder registerProcessingNotification(String processingEvent, RegisteredJacsNotification processingNotification) {
            if (processingNotification != null) {
                processingNotification.setEventName(processingEvent);
            }
            serviceExecutionContext.processingNotification = processingNotification;
            return this;
        }

        public Builder registerProcessingStageNotification(String processingStage, Optional<RegisteredJacsNotification> processingStageNotification) {
            processingStageNotification.ifPresent(n -> serviceExecutionContext.processingStageNotifications.put(processingStage, n));
            return this;
        }

        public ServiceExecutionContext build() {
            return serviceExecutionContext;
        }
    }

    private final JacsServiceData parentServiceData;
    private ProcessingLocation processingLocation;
    private String serviceName;
    private String outputPath;
    private String errorPath;
    private String workspace;
    private JacsServiceState serviceState;
    private String description;
    private final List<JacsServiceData> waitFor = new ArrayList<>();
    private final List<Number> waitForIds = new ArrayList<>();
    private final Map<String, String> resources = new LinkedHashMap<>();
    private RegisteredJacsNotification processingNotification;
    private final Map<String, RegisteredJacsNotification> processingStageNotifications = new HashMap<>();

    private ServiceExecutionContext(JacsServiceData parentServiceData) {
        this.parentServiceData = parentServiceData;
    }

    JacsServiceData getParentServiceData() {
        return parentServiceData;
    }

    ProcessingLocation getProcessingLocation() {
        return processingLocation;
    }

    List<JacsServiceData> getWaitFor() {
        return waitFor;
    }

    List<Number> getWaitForIds() {
        return waitForIds;
    }

    JacsServiceState getServiceState() {
        return serviceState;
    }

    String getServiceName() {
        return serviceName;
    }

    String getOutputPath() {
        return outputPath;
    }

    String getErrorPath() {
        return errorPath;
    }

    String getWorkspace() {
        return workspace;
    }

    String getParentWorkspace() {
        return parentServiceData != null ? parentServiceData.getWorkspace() : null;
    }

    String getDescription() {
        return description;
    }

    Map<String, String> getResources() {
        return resources;
    }

    RegisteredJacsNotification getProcessingNotification() {
        return processingNotification;
    }

    Map<String, RegisteredJacsNotification> getProcessingStageNotifications() {
        return processingStageNotifications;
    }
}

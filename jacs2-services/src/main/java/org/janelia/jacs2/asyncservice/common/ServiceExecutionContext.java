package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
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

        public Builder(JacsServiceData parentService) {
            serviceExecutionContext = new ServiceExecutionContext(parentService);
        }

        public Builder processingLocation(ProcessingLocation processingLocation) {
            if (processingLocation != null) serviceExecutionContext.processingLocation = processingLocation;
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

        public Builder setServiceTimeoutInMillis(Long timeoutInMillis) {
            serviceExecutionContext.serviceTimeoutInMillis = timeoutInMillis;
            return this;
        }

        public Builder setServiceName(String serviceName) {
            serviceExecutionContext.serviceName = serviceName;
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

        public Builder addDictionaryArgs(Map<String, Object> dictionaryArgs) {
            if (dictionaryArgs != null) {
                serviceExecutionContext.dictionaryArgs.putAll(dictionaryArgs);
            }
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

    private final JacsServiceData parentService;
    private ProcessingLocation processingLocation;
    private String serviceName;
    private String workspace;
    private JacsServiceState serviceState;
    private Long serviceTimeoutInMillis;
    private String description;
    private final List<JacsServiceData> waitFor = new ArrayList<>();
    private final List<Number> waitForIds = new ArrayList<>();
    private final Map<String, Object> dictionaryArgs = new LinkedHashMap<>();
    private final Map<String, String> resources = new LinkedHashMap<>();
    private RegisteredJacsNotification processingNotification;
    private final Map<String, RegisteredJacsNotification> processingStageNotifications = new HashMap<>();

    private ServiceExecutionContext(JacsServiceData parentService) {
        if (parentService == null || parentService.hasId()) {
            this.parentService = parentService;
        } else {
            this.parentService = parentService.getParentService();
        }
        if (parentService != null) {
            ResourceHelper.setAuthToken(resources, ResourceHelper.getAuthToken(parentService.getResources()));
        }
    }

    JacsServiceData getParentService() {
        return parentService;
    }

    ProcessingLocation getProcessingLocation() {
        if (processingLocation != null) {
            return processingLocation;
        } else if (parentService != null) {
            return parentService.getProcessingLocation();
        } else {
            return null;
        }
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

    Long getServiceTimeoutInMillis() {
        return serviceTimeoutInMillis;
    }

    String getServiceName(String defaultServiceName) {
        return StringUtils.defaultIfBlank(serviceName, defaultServiceName);
    }

    String getWorkspace() {
        if (StringUtils.isNotBlank(workspace)) {
            return workspace;
        } else if (parentService != null) {
            return parentService.getWorkspace();
        } else {
            return null;
        }
    }

    String getDescription() {
        return description;
    }

    Map<String, String> getResourcesFromParent() {
        return parentService != null ? ImmutableMap.copyOf(parentService.getResources()) : ImmutableMap.of();
    }
    Map<String, String> getResources() {
        return resources;
    }

    Map<String, Object> getDictionaryArgs() {
        return dictionaryArgs;
    }

    RegisteredJacsNotification getProcessingNotification() {
        return processingNotification;
    }

    Map<String, RegisteredJacsNotification> getProcessingStageNotifications() {
        return processingStageNotifications;
    }
}

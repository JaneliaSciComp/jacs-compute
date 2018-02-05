package org.janelia.model.service;

import java.util.Map;

public class JacsServiceDataBuilder {

    private final JacsServiceData serviceData;

    public JacsServiceDataBuilder(JacsServiceData serviceContext) {
        this.serviceData = new JacsServiceData();
        if (serviceContext != null) {
            serviceData.setOwnerKey(serviceContext.getOwnerKey());
            serviceData.updateParentService(serviceContext);
            if (serviceContext.getProcessingLocation() != null) {
                serviceData.setProcessingLocation(serviceContext.getProcessingLocation());
            }
            serviceData.setWorkspace(serviceContext.getWorkspace());
            serviceData.setTags(serviceContext.getTags()); // propagate the tags
        }
    }

    public JacsServiceDataBuilder addArg(String... args) {
        for (String arg : args) {
            serviceData.addArg(arg);
        }
        return this;
    }

    public JacsServiceDataBuilder clearArgs() {
        serviceData.clearArgs();
        return this;
    }

    public JacsServiceDataBuilder setName(String name) {
        serviceData.setName(name);
        return this;
    }

    public JacsServiceDataBuilder setOwnerKey(String ownerKey) {
        serviceData.setOwnerKey(ownerKey);
        return this;
    }

    public JacsServiceDataBuilder setProcessingLocation(ProcessingLocation processingLocation) {
        serviceData.setProcessingLocation(processingLocation);
        return this;
    }

    public JacsServiceDataBuilder setState(JacsServiceState state) {
        serviceData.setState(state);
        return this;
    }

    public JacsServiceDataBuilder addDependency(JacsServiceData serviceDependency) {
        serviceData.addServiceDependency(serviceDependency);
        return this;
    }

    public JacsServiceDataBuilder addDependencyId(Number serviceDependencyId) {
        serviceData.addServiceDependencyId(serviceDependencyId);
        return this;
    }

    public JacsServiceDataBuilder setWorkspace(String workspace) {
        serviceData.setWorkspace(workspace);
        return this;
    }

    public JacsServiceDataBuilder setOutputPath(String outputPath) {
        serviceData.setOutputPath(outputPath);
        return this;
    }

    public JacsServiceDataBuilder setErrorPath(String errorPath) {
        serviceData.setErrorPath(errorPath);
        return this;
    }

    public JacsServiceDataBuilder setDescription(String description) {
        serviceData.setDescription(description);
        return this;
    }

    public JacsServiceDataBuilder copyResourcesFrom(Map<String, String> resources) {
        serviceData.getResources().putAll(resources);
        return this;
    }

    public JacsServiceDataBuilder registerProcessingNotification(RegisteredJacsNotification notification) {
        serviceData.setProcessingNotification(notification);
        return this;
    }

    public JacsServiceDataBuilder registerProcessingStageNotification(String processingStage, RegisteredJacsNotification notification) {
        serviceData.setProcessingStageNotification(processingStage, notification);
        return this;
    }

    public JacsServiceDataBuilder registerProcessingStageNotifications(Map<String, RegisteredJacsNotification> notifications) {
        serviceData.setProcessingStagedNotifications(notifications);
        return this;
    }

    public JacsServiceData build() {
        return serviceData;
    }
}

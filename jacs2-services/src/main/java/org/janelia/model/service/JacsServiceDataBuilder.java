package org.janelia.model.service;

import org.janelia.jacs2.asyncservice.common.ResourceHelper;

import java.util.List;
import java.util.Map;

public class JacsServiceDataBuilder {

    private final JacsServiceData serviceData;

    public JacsServiceDataBuilder(JacsServiceData serviceContext) {
        this.serviceData = new JacsServiceData();
        this.serviceData.initAccessId();
        if (serviceContext != null) {
            serviceData.setOwnerKey(serviceContext.getOwnerKey());
            serviceData.setAuthKey(serviceContext.getAuthKey());
            serviceData.updateParentService(serviceContext);
            if (serviceContext.getProcessingLocation() != null) {
                serviceData.setProcessingLocation(serviceContext.getProcessingLocation());
            }
            // propagate the queue ID from the parent service so that the entire service tree
            // is processed on the same host
            serviceData.setQueueId(serviceContext.getQueueId());
            serviceData.setWorkspace(serviceContext.getWorkspace());
            serviceData.setTags(serviceContext.getTags()); // propagate the tags
            ResourceHelper.setAuthToken(serviceData.getResources(), ResourceHelper.getAuthToken(serviceContext.getResources()));
        }
    }

    public JacsServiceDataBuilder addArgs(String... args) {
        for (String arg : args) {
            serviceData.addArg(arg);
        }
        return this;
    }

    public JacsServiceDataBuilder addArgs(List<String> args) {
        for (String arg : args) {
            serviceData.addArg(arg);
        }
        return this;
    }

    public JacsServiceDataBuilder setDictionaryArgs(Map<String, Object> dictionaryArgs) {
        serviceData.setDictionaryArgs(dictionaryArgs);
        return this;
    }

    public JacsServiceDataBuilder clearArgs() {
        serviceData.clearArgs();
        return this;
    }

    public JacsServiceDataBuilder addServiceArg(String name, Object value) {
        serviceData.addServiceArg(name, value);
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

    public JacsServiceDataBuilder setAuthKey(String ownerKey) {
        serviceData.setAuthKey(ownerKey);
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

    public JacsServiceDataBuilder addResource(String name, String value) {
        serviceData.getResources().put(name, value);
        return this;
    }

    public JacsServiceDataBuilder addResources(Map<String, String> resources) {
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

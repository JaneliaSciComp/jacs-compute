package org.janelia.model.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.model.domain.entityannotations.EntityId;
import org.janelia.model.jacs2.BaseEntity;
import org.janelia.model.jacs2.domain.interfaces.HasIdentifier;
import org.janelia.model.jacs2.domain.support.MongoMapping;

@MongoMapping(collectionName="jacsScheduledService", label="JacsScheduledService")
public class JacsScheduledServiceData implements BaseEntity, HasIdentifier {

    @EntityId
    private Number id;
    private String name; // scheduled job name
    private String description; // scheduled job description
    private String serviceName; // service to be run
    private String serviceVersion;
    private Integer servicePriority = 0; // service priority
    private String runServiceAs; // whom should this service run as
    private ProcessingLocation serviceProcessingLocation; // service processing location
    private String serviceQueueId;
    private List<String> serviceArgs = new ArrayList<>(); // service command line args
    private Map<String, Object> serviceDictionaryArgs = new LinkedHashMap<>(); // service dictionary args
    private Map<String, String> serviceResources = new LinkedHashMap<>(); // service resources
    private String cronScheduleDescriptor; // crontab like descriptor
    private Date lastStartTime; // last time this scheduled service was started
    private Date nextStartTime; // next time this scheduled service needs to run
    private boolean disabled; // quick disable flag
    private Set<String> successEmailNotifications = new LinkedHashSet<>();
    private Set<String> failureEmailNotifications = new LinkedHashSet<>();

    @Override
    public Number getId() {
        return id;
    }

    @Override
    public void setId(Number id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getServiceVersion() {
        return serviceVersion;
    }

    public void setServiceVersion(String serviceVersion) {
        this.serviceVersion = serviceVersion;
    }

    public Integer getServicePriority() {
        return servicePriority;
    }

    public void setServicePriority(Integer servicePriority) {
        this.servicePriority = servicePriority;
    }

    public String getRunServiceAs() {
        return runServiceAs;
    }

    public void setRunServiceAs(String runServiceAs) {
        this.runServiceAs = runServiceAs;
    }

    public ProcessingLocation getServiceProcessingLocation() {
        return serviceProcessingLocation;
    }

    public void setServiceProcessingLocation(ProcessingLocation serviceProcessingLocation) {
        this.serviceProcessingLocation = serviceProcessingLocation;
    }

    public String getServiceQueueId() {
        return serviceQueueId;
    }

    public void setServiceQueueId(String serviceQueueId) {
        this.serviceQueueId = serviceQueueId;
    }

    public List<String> getServiceArgs() {
        return serviceArgs;
    }

    public void setServiceArgs(List<String> serviceArgs) {
        this.serviceArgs = serviceArgs;
    }

    public Map<String, Object> getServiceDictionaryArgs() {
        return serviceDictionaryArgs;
    }

    public void setServiceDictionaryArgs(Map<String, Object> serviceDictionaryArgs) {
        this.serviceDictionaryArgs = serviceDictionaryArgs;
    }

    public Map<String, String> getServiceResources() {
        return serviceResources;
    }

    public void setServiceResources(Map<String, String> serviceResources) {
        this.serviceResources = serviceResources;
    }

    public String getCronScheduleDescriptor() {
        return cronScheduleDescriptor;
    }

    public void setCronScheduleDescriptor(String cronScheduleDescriptor) {
        this.cronScheduleDescriptor = cronScheduleDescriptor;
    }

    public Date getLastStartTime() {
        return lastStartTime;
    }

    public void setLastStartTime(Date lastStartTime) {
        this.lastStartTime = lastStartTime;
    }

    public Date getNextStartTime() {
        return nextStartTime;
    }

    public void setNextStartTime(Date nextStartTime) {
        this.nextStartTime = nextStartTime;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    public Set<String> getSuccessEmailNotifications() {
        return successEmailNotifications;
    }

    public void setSuccessEmailNotifications(Set<String> successEmailNotifications) {
        this.successEmailNotifications = successEmailNotifications;
    }

    public Set<String> getFailureEmailNotifications() {
        return failureEmailNotifications;
    }

    public void setFailureEmailNotifications(Set<String> failureEmailNotifications) {
        this.failureEmailNotifications = failureEmailNotifications;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("name", name)
                .append("description", description)
                .append("serviceName", serviceName)
                .append("serviceVersion", serviceVersion)
                .append("servicePriority", servicePriority)
                .append("runServiceAs", runServiceAs)
                .append("serviceProcessingLocation", serviceProcessingLocation)
                .append("serviceQueueId", serviceQueueId)
                .append("serviceArgs", serviceArgs)
                .append("serviceDictionaryArgs", serviceDictionaryArgs)
                .append("serviceResources", serviceResources)
                .append("cronScheduleDescriptor", cronScheduleDescriptor)
                .append("lastStartTime", lastStartTime)
                .append("nextStartTime", nextStartTime)
                .append("disabled", disabled)
                .toString();
    }

    public JacsServiceData createServiceInstance() {
        JacsServiceData serviceData = new JacsServiceDataBuilder(null)
                .setName(serviceName)
                .setDescription("Triggered by " + name + "#" + id + (StringUtils.isNotBlank(description) ? ": " + description : ""))
                .setOwnerKey(runServiceAs)
                .setAuthKey(runServiceAs)
                .setProcessingLocation(serviceProcessingLocation)
                .setVersion(serviceVersion)
                .addArgs(serviceArgs)
                .setDictionaryArgs(serviceDictionaryArgs)
                .addResources(serviceResources)
                .addSuccessEmailNotifications(successEmailNotifications)
                .addFailureEmailNotifications(failureEmailNotifications)
                .build();
        serviceData.setQueueId(serviceQueueId);
        serviceData.setPriority(servicePriority);
        serviceData.addTags(name + "#" + id);
        return serviceData;
    }
}

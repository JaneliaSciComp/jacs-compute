package org.janelia.jacs2.model.jacsservice;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RegisteredJacsNotification implements Serializable {
    public enum LifecycleStage {
        START_PROCESSING,
        SUCCESSFUL_PROCESSING,
        FAILED_PROCESSING
    }

    /**
     * processingStage is typically used for passing the registered notification to sub-services. If it is not set then
     * the notification is sent by the current processing in accordance with the specified lifecycle stages.
     */
    private String processingStage;
    private Set<LifecycleStage> registeredLifecycleStages = new HashSet<>();
    private Map<String, String> notificationData = new LinkedHashMap<>();

    public RegisteredJacsNotification() {
        // Empty ctor for serialization
    }

    public RegisteredJacsNotification(String processingStage) {
        this.processingStage = processingStage;
    }

    public String getProcessingStage() {
        return processingStage;
    }

    public void setProcessingStage(String processingStage) {
        this.processingStage = processingStage;
    }

    public Set<LifecycleStage> getRegisteredLifecycleStages() {
        return registeredLifecycleStages;
    }

    public void setRegisteredLifecycleStages(Set<LifecycleStage> registeredLifecycleStages) {
        this.registeredLifecycleStages = registeredLifecycleStages;
    }

    public RegisteredJacsNotification withDefaultLifecycleStages() {
        registeredLifecycleStages.addAll(EnumSet.allOf(LifecycleStage.class));
        return this;
    }

    public RegisteredJacsNotification forLifecycleStage(Set<LifecycleStage> lifecycleStages) {
        for (LifecycleStage lifecycleStage : lifecycleStages)
            registeredLifecycleStages.add(lifecycleStage);
        return this;
    }

    public RegisteredJacsNotification forLifecycleStage(LifecycleStage ...lifecycleStages) {
        for (LifecycleStage lifecycleStage : lifecycleStages)
            registeredLifecycleStages.add(lifecycleStage);
        return this;
    }

    public Map<String, String> getNotificationData() {
        return notificationData;
    }

    public void setNotificationData(Map<String, String> notificationData) {
        this.notificationData = notificationData;
    }

    public RegisteredJacsNotification addNotificationField(String name, String value) {
        notificationData.put(name, value);
        return this;
    }

    public RegisteredJacsNotification addNotificationField(String name, Number value) {
        notificationData.put(name, value.toString());
        return this;
    }

    public RegisteredJacsNotification addNotificationFields(Map<String, String> notificationData) {
        this.notificationData.putAll(notificationData);
        return this;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        RegisteredJacsNotification that = (RegisteredJacsNotification) o;

        return new EqualsBuilder()
                .append(processingStage, that.processingStage)
                .append(notificationData, that.notificationData)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(processingStage)
                .append(notificationData)
                .toHashCode();
    }
}

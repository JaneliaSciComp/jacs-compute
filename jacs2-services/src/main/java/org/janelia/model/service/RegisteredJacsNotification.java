package org.janelia.model.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class RegisteredJacsNotification implements Serializable {

    private String eventName;
    private Set<JacsServiceLifecycleStage> registeredLifecycleStages = new HashSet<>();
    private Map<String, String> notificationData = new LinkedHashMap<>();

    public RegisteredJacsNotification() {
        // Empty ctor
    }

    public RegisteredJacsNotification(RegisteredJacsNotification n) {
        this.eventName = n.eventName;
        registeredLifecycleStages.addAll(n.registeredLifecycleStages);
        notificationData.putAll(n.notificationData);
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public RegisteredJacsNotification withEventName(String eventName) {
        this.eventName = eventName;
        return this;
    }

    public Set<JacsServiceLifecycleStage> getRegisteredLifecycleStages() {
        return registeredLifecycleStages;
    }

    public void setRegisteredLifecycleStages(Set<JacsServiceLifecycleStage> registeredLifecycleStages) {
        this.registeredLifecycleStages = registeredLifecycleStages;
    }

    public RegisteredJacsNotification withDefaultLifecycleStages() {
        registeredLifecycleStages.addAll(EnumSet.allOf(JacsServiceLifecycleStage.class));
        return this;
    }

    public RegisteredJacsNotification forLifecycleStage(Set<JacsServiceLifecycleStage> lifecycleStages) {
        for (JacsServiceLifecycleStage lifecycleStage : lifecycleStages)
            registeredLifecycleStages.add(lifecycleStage);
        return this;
    }

    public RegisteredJacsNotification forLifecycleStage(JacsServiceLifecycleStage...lifecycleStages) {
        for (JacsServiceLifecycleStage lifecycleStage : lifecycleStages)
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
        if (StringUtils.isNotBlank(value))
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
                .append(eventName, that.eventName)
                .append(notificationData, that.notificationData)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(eventName)
                .append(notificationData)
                .toHashCode();
    }
}

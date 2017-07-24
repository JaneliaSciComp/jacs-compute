package org.janelia.jacs2.model.jacsservice;

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

public class JacsNotification implements Serializable {
    public enum LifecycleStage {
        START_PROCESSING,
        SUCCESSFUL_PROCESSING,
        FAILED_PROCESSING
    }

    private Set<LifecycleStage> registeredLifecycleStages = new HashSet<>();
    private Map<String, String> notificationData = new LinkedHashMap<>();

    public JacsNotification() {
        // Empty ctor
    }

    public JacsNotification(JacsNotification n) {
        registeredLifecycleStages.addAll(n.registeredLifecycleStages);
        notificationData.putAll(n.notificationData);
    }

    public Set<LifecycleStage> getRegisteredLifecycleStages() {
        return registeredLifecycleStages;
    }

    public void setRegisteredLifecycleStages(Set<LifecycleStage> registeredLifecycleStages) {
        this.registeredLifecycleStages = registeredLifecycleStages;
    }

    public JacsNotification withDefaultLifecycleStages() {
        registeredLifecycleStages.addAll(EnumSet.allOf(LifecycleStage.class));
        return this;
    }

    public JacsNotification forLifecycleStage(Set<LifecycleStage> lifecycleStages) {
        for (LifecycleStage lifecycleStage : lifecycleStages)
            registeredLifecycleStages.add(lifecycleStage);
        return this;
    }

    public JacsNotification forLifecycleStage(LifecycleStage ...lifecycleStages) {
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

    public JacsNotification addNotificationField(String name, String value) {
        if (StringUtils.isNotBlank(value))
            notificationData.put(name, value);
        return this;
    }

    public JacsNotification addNotificationField(String name, Number value) {
        notificationData.put(name, value.toString());
        return this;
    }

    public JacsNotification addNotificationFields(Map<String, String> notificationData) {
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

        JacsNotification that = (JacsNotification) o;

        return new EqualsBuilder()
                .append(notificationData, that.notificationData)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(notificationData)
                .toHashCode();
    }
}

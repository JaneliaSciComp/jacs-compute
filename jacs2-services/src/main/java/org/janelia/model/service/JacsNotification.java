package org.janelia.model.service;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.model.jacs2.domain.annotations.EntityId;
import org.janelia.model.jacs2.domain.interfaces.HasIdentifier;
import org.janelia.model.jacs2.domain.support.MongoMapping;
import org.janelia.model.jacs2.BaseEntity;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

@MongoMapping(collectionName="jacsNotification", label="Jacs Notification")
public class JacsNotification implements BaseEntity, HasIdentifier {

    @EntityId
    private Number id;
    private String eventName;
    private Date notificationDate = new Date();
    private JacsServiceLifecycleStage notificationStage;
    private Map<String, String> notificationData = new LinkedHashMap<>();

    @Override
    public Number getId() {
        return id;
    }

    @Override
    public void setId(Number id) {
        this.id = id;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public Date getNotificationDate() {
        return notificationDate;
    }

    public void setNotificationDate(Date notificationDate) {
        this.notificationDate = notificationDate;
    }

    public JacsServiceLifecycleStage getNotificationStage() {
        return notificationStage;
    }

    public void setNotificationStage(JacsServiceLifecycleStage notificationStage) {
        this.notificationStage = notificationStage;
    }

    public Map<String, String> getNotificationData() {
        return notificationData;
    }

    public void setNotificationData(Map<String, String> notificationData) {
        if (notificationData != null) {
            this.notificationData.putAll(notificationData);
        }
    }

    public void addNotificationData(String name, String value) {
        this.notificationData.put(name, value);
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
                .append(id, that.id)
                .append(notificationStage, that.notificationStage)
                .append(notificationData, that.notificationData)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(notificationStage)
                .append(notificationData)
                .toHashCode();
    }
}

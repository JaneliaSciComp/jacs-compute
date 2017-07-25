package org.janelia.jacs2.model.jacsservice;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.it.jacs.model.domain.interfaces.HasIdentifier;
import org.janelia.it.jacs.model.domain.support.MongoMapping;
import org.janelia.jacs2.model.BaseEntity;

import java.util.Date;
import java.util.Map;

@MongoMapping(collectionName="jacsNotification", label="Jacs Notification")
public class JacsNotification implements BaseEntity, HasIdentifier {

    @JsonProperty("_id")
    private Number id;
    private Date notificationDate = new Date();
    private JacsServiceLifecycleStage notificationStage;
    private Map<String, String> notificationData;

    @Override
    public Number getId() {
        return id;
    }

    @Override
    public void setId(Number id) {
        this.id = id;
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
        this.notificationData = notificationData;
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

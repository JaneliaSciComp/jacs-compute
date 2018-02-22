package org.janelia.model.access.dao;

import org.janelia.model.service.JacsScheduledServiceData;

import java.util.Date;
import java.util.List;
import java.util.Optional;

public interface JacsScheduledServiceDataDao extends ReadWriteDao<JacsScheduledServiceData, Number> {
    List<JacsScheduledServiceData> findServicesScheduledAtOrBefore(String queueId, Date scheduledTime, boolean includeDisabled);
    Optional<JacsScheduledServiceData> updateServiceScheduledTime(JacsScheduledServiceData scheduledServiceData, Date currentScheduledTime);
}

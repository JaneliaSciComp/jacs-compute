package org.janelia.jacs2.asyncservice;

import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.service.JacsServiceData;

import java.util.Date;

public interface JacsServiceDataManager {
    JacsServiceData retrieveServiceById(Number instanceId);
    long countServices(JacsServiceData ref, DataInterval<Date> creationInterval);
    PageResult<JacsServiceData> searchServices(JacsServiceData ref, DataInterval<Date> creationInterval, PageRequest pageRequest);
    JacsServiceData updateService(Number instanceId, JacsServiceData serviceData);
}

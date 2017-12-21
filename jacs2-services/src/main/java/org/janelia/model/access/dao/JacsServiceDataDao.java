package org.janelia.model.access.dao;

import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.service.JacsServiceEvent;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;

import java.util.Date;
import java.util.List;
import java.util.Set;

public interface JacsServiceDataDao extends ReadWriteDao<JacsServiceData, Number> {
    List<JacsServiceData> findChildServices(Number serviceId);
    /**
     * Returns the service hierarchy for the service identified by <code>serviceId</code>
     * @param serviceId service identifier
     * @return service the service with all its dependencies
     */
    JacsServiceData findServiceHierarchy(Number serviceId);
    PageResult<JacsServiceData> findMatchingServices(JacsServiceData pattern, DataInterval<Date> creationInterval, PageRequest pageRequest);
    PageResult<JacsServiceData> findServicesByState(Set<JacsServiceState> requestStates, PageRequest pageRequest);
    PageResult<JacsServiceData> claimServiceByQueueAndState(String queueId, Set<JacsServiceState> requestStates, PageRequest pageRequest);
    void saveServiceHierarchy(JacsServiceData serviceData);
    void addServiceEvent(JacsServiceData jacsServiceData, JacsServiceEvent serviceEvent);
    void updateServiceResult(JacsServiceData serviceData);
}

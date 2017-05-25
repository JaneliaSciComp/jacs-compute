package org.janelia.jacs2.dao;

import org.janelia.jacs2.model.DataInterval;
import org.janelia.jacs2.model.jacsservice.JacsServiceEvent;
import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.page.PageResult;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;

import java.util.Date;
import java.util.List;
import java.util.Map;
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
    void archiveServiceHierarchy(JacsServiceData serviceData);
    void addServiceEvent(JacsServiceData jacsServiceData, JacsServiceEvent serviceEvent);
}

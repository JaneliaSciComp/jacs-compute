package org.janelia.model.access.dao;

import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.service.JacsServiceData;

import java.util.Date;

/**
 * Archived JacsServiceData read/write access spec.
 */
public interface JacsServiceDataArchiveDao extends ArchiveDao<JacsServiceData, Number>  {
    JacsServiceData findArchivedServiceHierarchy(Number serviceId);
    PageResult<JacsServiceData> findMatchingServices(JacsServiceData pattern, DataInterval<Date> creationInterval, PageRequest pageRequest);
}

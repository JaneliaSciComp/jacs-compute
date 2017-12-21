package org.janelia.model.access.dao;

import org.janelia.model.service.JacsServiceData;

/**
 * Archived JacsServiceData read/write access spec.
 */
public interface JacsServiceDataArchiveDao extends ArchiveDao<JacsServiceData, Number>  {
    JacsServiceData findArchivedServiceHierarchy(Number serviceId);
}

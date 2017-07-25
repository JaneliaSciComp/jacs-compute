package org.janelia.jacs2.dao;

import org.janelia.jacs2.model.DataInterval;
import org.janelia.jacs2.model.jacsservice.JacsNotification;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEvent;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.janelia.jacs2.model.page.PageRequest;
import org.janelia.jacs2.model.page.PageResult;

import java.util.Date;
import java.util.List;
import java.util.Set;

public interface JacsNotificationDao extends ReadWriteDao<JacsNotification, Number> {
}

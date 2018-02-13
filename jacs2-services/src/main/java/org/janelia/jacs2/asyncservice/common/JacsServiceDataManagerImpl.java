package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.jacs2.SetFieldValueHandler;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.service.JacsServiceData;
import org.janelia.jacs2.asyncservice.JacsServiceDataManager;

import javax.inject.Inject;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class JacsServiceDataManagerImpl implements JacsServiceDataManager {

    private final JacsServiceDataPersistence jacsServiceDataPersistence;

    @Inject
    public JacsServiceDataManagerImpl(JacsServiceDataPersistence jacsServiceDataPersistence) {
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
    }

    @Override
    public JacsServiceData retrieveServiceById(Number instanceId) {
        return jacsServiceDataPersistence.findServiceHierarchy(instanceId);
    }

    @Override
    public PageResult<JacsServiceData> searchServices(JacsServiceData ref, DataInterval<Date> creationInterval, PageRequest pageRequest) {
        return jacsServiceDataPersistence.findMatchingServices(ref, creationInterval, pageRequest);
    }

    @Override
    public JacsServiceData updateService(Number instanceId, JacsServiceData serviceData) {
        JacsServiceData existingService = jacsServiceDataPersistence.findServiceHierarchy(instanceId);
        if (existingService == null) {
            return null;
        }
        Map<String, EntityFieldValueHandler<?>> updates = new LinkedHashMap<>();
        if (serviceData.getState() != null) {
            existingService.setState(serviceData.getState());
            updates.put("state", new SetFieldValueHandler<>(serviceData.getState()));
        }
        if (serviceData.getServiceTimeout() != null) {
            existingService.setServiceTimeout(serviceData.getServiceTimeout());
            updates.put("serviceTimeout", new SetFieldValueHandler<>(serviceData.getServiceTimeout()));
        }
        if (StringUtils.isNotBlank(serviceData.getWorkspace())) {
            existingService.setWorkspace(serviceData.getWorkspace());
            updates.put("workspace", new SetFieldValueHandler<>(serviceData.getWorkspace()));
        }
        if (!updates.isEmpty()) {
            jacsServiceDataPersistence.update(existingService, updates);
        }
        if (serviceData.getPriority() != null) {
            Map<JacsServiceData, Integer> newPriorities = existingService.getNewServiceHierarchyPriorities(serviceData.getPriority());
            newPriorities.entrySet().forEach(sdpEntry -> {
                JacsServiceData sd = sdpEntry.getKey();
                sd.setPriority(sdpEntry.getValue());
                jacsServiceDataPersistence.update(sd, ImmutableMap.of("priority", new SetFieldValueHandler<>(sd.getPriority())));
            });
        }
        return existingService;
    }

}

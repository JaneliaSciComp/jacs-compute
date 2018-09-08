package org.janelia.jacs2.utils;

import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Singleton which offers access to metadata about the currently executing service, based on the executing thread.
 *
 * This only works for services extending AbstractServiceProcessor2.
 */
@Singleton
public class CurrentService {

    private static final String ERROR_MESSAGE = "CurrentService is not set. " +
            "Make sure you're using the AbstractServiceProcessor2 service hierarchy.";

    private final ThreadLocal<JacsServiceData> threadLocal = new ThreadLocal<>();

    @Inject
    private JacsServiceDataPersistence jacsServiceDataPersistence;

    public JacsServiceData getJacsServiceData() {
        JacsServiceData sd = threadLocal.get();
        if (sd==null) {
            throw new ComputationException(ERROR_MESSAGE);
        }
        return sd;
    }

    public Object getInput(String key) {
        return getJacsServiceData().getDictionaryArgs().get(key);
    }

    public Long getId() {
        return getJacsServiceData().getId().longValue();
    }

    public Long getParentId() {
        JacsServiceData sd = getJacsServiceData();
        return sd.getParentServiceId() == null ? null : sd.getParentServiceId().longValue();
    }

    public Long getRootId() {
        JacsServiceData sd = getJacsServiceData();
        return sd.getRootServiceId() == null ? null : sd.getRootServiceId().longValue();
    }

    public String getOwnerKey() {
        return getJacsServiceData().getOwnerKey();
    }

    public String getOwnerName() {
        return getJacsServiceData().getOwnerName();
    }

    public void setJacsServiceData(JacsServiceData jacsServiceData) {
        // Refresh the service data every time it's set.
        // Inefficient, but at least we know it's fresh.
        threadLocal.set(refresh(jacsServiceData));
    }

    private JacsServiceData refresh(JacsServiceData sd) {
        JacsServiceData refreshedSd = jacsServiceDataPersistence.findById(sd.getId());
        if (refreshedSd == null) {
            throw new IllegalStateException("Service data not found for "+sd.getId());
        }
        return refreshedSd;
    }
}
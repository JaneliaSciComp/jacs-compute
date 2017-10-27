package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsServiceData;

import java.util.List;

public interface JacsServiceQueue {
    int getMaxReadyCapacity();
    void setMaxReadyCapacity(int maxReadyCapacity);
    JacsServiceData enqueueService(JacsServiceData jacsServiceData);
    JacsServiceData dequeService();
    void refreshServiceQueue();
    void abortService(JacsServiceData jacsServiceData);
    void completeService(JacsServiceData jacsServiceData);
    int getReadyServicesSize();
    int getPendingServicesSize();
    List<Number> getPendingServices();
}

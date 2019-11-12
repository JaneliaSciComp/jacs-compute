package org.janelia.jacs2.asyncservice.common.resulthandlers;

import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.model.service.JacsServiceData;

public abstract class AbstractEmptyServiceResultHandler<T> implements ServiceResultHandler<T> {

    @Override
    public T collectResult(JacsServiceData jacsServiceData) {
        return null;
    }

    @Override
    public void updateServiceDataResult(JacsServiceData jacsServiceData, T result) {
        // do nothing
    }

    @Override
    public T getServiceDataResult(JacsServiceData jacsServiceData) {
        return null;
    }
}

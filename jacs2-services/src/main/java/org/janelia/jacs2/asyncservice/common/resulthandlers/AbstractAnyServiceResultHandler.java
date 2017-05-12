package org.janelia.jacs2.asyncservice.common.resulthandlers;

import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;

public abstract class AbstractAnyServiceResultHandler<T> implements ServiceResultHandler<T> {

    @Override
    public void updateServiceDataResult(JacsServiceData jacsServiceData, T result) {
        jacsServiceData.setSerializableResult(ServiceDataUtils.anyToSerializableObject(result));
    }

}

package org.janelia.jacs2.asyncservice.common.resulthandlers;

import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.spark.SparkClusterStartProcessor;
import org.janelia.model.service.JacsServiceData;

public abstract class AbstractAnyServiceResultHandler<T> implements ServiceResultHandler<T> {

    @Override
    public T collectResult(JacsServiceData jacsServiceData) {
        return getServiceDataResult(jacsServiceData);
    }

    @Override
    public void updateServiceDataResult(JacsServiceData jacsServiceData, T result) {
        jacsServiceData.setSerializableResult(result);
    }

}

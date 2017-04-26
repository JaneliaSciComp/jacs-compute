package org.janelia.jacs2.asyncservice.common.resulthandlers;

import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;

import java.util.Optional;

public class EmptyServiceResultHandler<T> implements ServiceResultHandler<T> {
    @Override
    public boolean isResultReady(JacsServiceResult<?> depResults) {
        return true;
    }

    @Override
    public T collectResult(JacsServiceResult<?> depResults) {
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

    @Override
    public Optional<T> getExpectedServiceResult(JacsServiceData jacsServiceData) {
        return Optional.ofNullable(null);
    }
}

package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsServiceData;

public interface ServiceResultHandler<R> {
    /**
     * Check if the service result is available.
     * @param depResults an intermediate result based on dependencies results
     * @return true if the result is available.
     */
    boolean isResultReady(JacsServiceResult<?> depResults);

    /**
     * Collect the service result.
     * @param depResults an intermediate result based on dependencies results
     * @return the service result.
     */
    R collectResult(JacsServiceResult<?> depResults);

    /**
     * Updates service data with the given result
     * @param jacsServiceData service data
     * @param result to be updated
     */
    void updateServiceDataResult(JacsServiceData jacsServiceData, R result);

    /**
     * Extract the result from the service data.
     * @param jacsServiceData service data
     * @return the result persisted in the given service data
     */
    R getServiceDataResult(JacsServiceData jacsServiceData);
}

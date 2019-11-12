package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsServiceData;

public interface ServiceResultHandler<R> {
    /**
     * Check if the service result is available.
     * @param jacsServiceData service data
     * @return true if the result is available.
     */
    boolean isResultReady(JacsServiceData jacsServiceData);

    /**
     * Collect the service result.
     * @param jacsServiceData service data
     * @return the service result.
     */
    R collectResult(JacsServiceData jacsServiceData);

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

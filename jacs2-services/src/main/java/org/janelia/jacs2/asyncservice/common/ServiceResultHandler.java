package org.janelia.jacs2.asyncservice.common;

import org.janelia.jacs2.model.jacsservice.JacsServiceData;

import java.util.Optional;

public interface ServiceResultHandler<T> {
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
    T collectResult(JacsServiceResult<?> depResults);

    /**
     * Updates service data with the given result
     * @param jacsServiceData service data
     * @param result to be updated
     */
    void updateServiceDataResult(JacsServiceData jacsServiceData, T result);

    /**
     * Extract the result from the service data.
     * @param jacsServiceData service data
     * @return the result persisted in the given service data
     */
    T getServiceDataResult(JacsServiceData jacsServiceData);

    /**
     * Return the result that the service is expected to return. This method is useful when the service returns
     * predefined file names for example so that callers of this service could take advantage and build a pipeline
     * based on this expected result. The method may not be able to actually compute the expected result in all situations.
     *
     * @param jacsServiceData service data
     * @return expected result
     */
    Optional<T> getExpectedServiceResult(JacsServiceData jacsServiceData);
}

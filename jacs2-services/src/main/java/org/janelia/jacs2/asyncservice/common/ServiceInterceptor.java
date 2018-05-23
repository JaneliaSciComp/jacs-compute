package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsServiceData;

/**
 * Multiple service interceptors can be added to a JACS service invocation in order to
 * provide AOP-like cross cutting functionality. Each interceptor can implement one or more of
 * the methods defined here, so their default implementations are empty.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public interface ServiceInterceptor {

    /**
     * Called when the service is first dispatched, before it suspends to wait
     * for any dependencies to complete.
     * @param jacsServiceData
     */
    default void onDispatch(JacsServiceData jacsServiceData) {
    }

    /**
     * Called before process(JacsServiceData) is called.
     * @param jacsServiceData
     */
    default void beforeProcess(JacsServiceData jacsServiceData) {
    }

    /**
     * Called right after process(JacsServiceData) is called, before the success or error is processed
     * in the JacsServiceData.
     * @param jacsServiceData
     */
    default void afterProcess(JacsServiceData jacsServiceData) {
    }

    /**
     * Called after all processing is completed, including any error processing.
     * @param jacsServiceData
     */
    default void andFinally(JacsServiceData jacsServiceData) {
    }

}

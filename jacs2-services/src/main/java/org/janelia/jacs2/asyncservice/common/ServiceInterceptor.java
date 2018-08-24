package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsServiceData;

/**
 * Multiple service interceptors can be added to a JACS service invocation in order to
 * provide AOP-like cross cutting functionality. Each interceptor can implement one or more of
 * the methods defined here, so their default implementations are empty.
 *
 *  The service data passed to these methods is guaranteed to be fresh from the database.
 *
 *  When implementing an interceptor, you must use the @Named annotation to give it a unique name, and then put that
 *  name into the JacsServiceData.interceptors list for any service which should be intercepted.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public interface ServiceInterceptor {

    /**
     * Called when the service is first dispatched, before it suspends to wait
     * for any dependencies to complete.
     * @param sd service data
     */
    default void onDispatch(JacsServiceData sd) {
    }

    /**
     * Called right before service processing is executed.
     * @param sd service data
     */
    default void beforeProcess(JacsServiceData sd) {
    }

    /**
     * Called right after the service processing is executed, before the success or error is processed
     * in the JacsServiceData.
     * @param sd service data
     */
    default void afterProcess(JacsServiceData sd) {
    }

    /**
     * Called after all processing is completed, including any error processing. Nothing else will happen to the
     * service after this method runs.
     * @param sd service data
     */
    default void andFinally(JacsServiceData sd) {
    }

}

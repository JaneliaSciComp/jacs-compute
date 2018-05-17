package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;

import java.util.Collection;

/**
 * Service processor parameterized on the result type
 * @param <R> result type
 */
public interface ServiceProcessor<R> {

    /**
     * @return service metadata
     */
    ServiceMetaData getMetadata();

    /**
     * Create service data based on the current execution context and the provided arguments.
     * @param executionContext current execution context.
     * @param args service arguments.
     * @return service data.
     */
    JacsServiceData createServiceData(ServiceExecutionContext executionContext, ServiceArg... args);

    /**
     * Create service data based on the current execution context and the provided arguments.
     * @param executionContext
     * @param args
     * @return
     */
    default JacsServiceData createServiceData(ServiceExecutionContext executionContext, Collection<ServiceArg> args) {
        return createServiceData(executionContext, args.toArray(new ServiceArg[args.size()]));
    }

    /**
     * @return service result handler
     */
    ServiceResultHandler<R> getResultHandler();

    /**
     * @return service error checker
     */
    ServiceErrorChecker getErrorChecker();

    ServiceComputation<JacsServiceResult<R>> process(JacsServiceData jacsServiceData);

    /**
     * Default process mechanism given the execution context and the service arguments.
     * @param executionContext current execution context.
     * @param args service arguments.
     * @return service information.
     */
    default ServiceComputation<JacsServiceResult<R>> process(ServiceExecutionContext executionContext, ServiceArg... args) {
        return process(createServiceData(executionContext, args));
    }
}

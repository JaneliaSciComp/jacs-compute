package org.janelia.jacs2.rest;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class IllegalStateRequestHandler implements ExceptionMapper<IllegalStateException> {
    private static final Logger LOG = LoggerFactory.getLogger(IllegalStateRequestHandler.class);

    @Override
    public Response toResponse(IllegalStateException exception) {
        LOG.error("Invalid state exception", exception);
        String errorMessage = exception.getMessage();
        if (StringUtils.isBlank(errorMessage)) {
            errorMessage = "Illegal state";
        }
        return Response
                .status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(new ErrorResponse(errorMessage))
                .build();
    }

}

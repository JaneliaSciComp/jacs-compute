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
        LOG.error("Invalid argument response", exception);
        String errorMessage = exception.getMessage();
        if (StringUtils.isBlank(errorMessage)) {
            errorMessage = "Illegal state";
        }
        return Response
                .status(Response.Status.BAD_REQUEST)
                .entity(new ErrorResponse(errorMessage))
                .build();
    }

}

package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.*;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.domain.dto.annotation.CreateAnnotationParams;
import org.janelia.model.domain.dto.annotation.QueryAnnotationParams;
import org.janelia.model.domain.dto.annotation.RemoveAnnotationParams;
import org.janelia.model.domain.dto.annotation.UpdateAnnotationParams;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.domain.dao.AnnotationDao;
import org.janelia.model.domain.ontology.Annotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;

@SwaggerDefinition(
        securityDefinition = @SecurityDefinition(
                apiKeyAuthDefinitions = {
                        @ApiKeyAuthDefinition(key = "user", name = "username", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER),
                        @ApiKeyAuthDefinition(key = "runAs", name = "runasuser", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER)
                }
        )
)
@Api(
        value = "Janelia Workstation Domain Data",
        authorizations = {
                @Authorization("user"),
                @Authorization("runAs")
        }
)
@RequireAuthentication
@ApplicationScoped
@Path("/data")
public class AnnotationResource {
    private static final Logger LOG = LoggerFactory.getLogger(AnnotationResource.class);

    @AsyncIndex
    @Inject
    private AnnotationDao annotationDao;

    @ApiOperation(value = "Creates an annotation",
            notes = "Creates a new annotation based on the given parameters"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully created annotation", response = Annotation.class),
            @ApiResponse(code = 500, message = "Internal Server Error creating Annotation")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("annotations")
    public Annotation createAnnotation(@ApiParam CreateAnnotationParams params) {
        LOG.trace("Start createAnnotation({})", params);
        try {
            Annotation annotation = annotationDao.createAnnotation(params.getSubjectKey(), params.getTarget(), params.getOntologyTermReference(), params.getValue());
            LOG.info("{} created annotation {} on {}: {}={}", params.getSubjectKey(), annotation.getId(), annotation.getTarget(), annotation.getKey(), annotation.getValue());
            return annotation;
        } catch (Exception e) {
            LOG.error("Error occurred creating annotation for {}", params, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished createAnnotation({})", params);
        }
    }

    @ApiOperation(value = "Updates an annotation",
            notes = "Updates the value of a given annotation"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated annotation", response = Annotation.class),
            @ApiResponse(code = 500, message = "Internal Server Error updated Annotation")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("annotations/value")
    public Annotation updateAnnotation(@ApiParam UpdateAnnotationParams params) {
        LOG.trace("Start updateAnnotation({})", params);
        try {
            Annotation annotation = annotationDao.updateAnnotationValue(params.getSubjectKey(), params.getAnnotationId(), params.getValue());
            LOG.info("{} updated annotation {} on {}: {}={}", params.getSubjectKey(), annotation.getId(), annotation.getTarget(), annotation.getKey(), annotation.getValue());
            return annotation;
        } catch (Exception e) {
            LOG.error("Error occurred updating annotation {}", params, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished updateAnnotation({})", params);
        }
    }

    @ApiOperation(value = "Returns a list of Annotations",
            notes = "Returns a list of Annotations for the objects given in the references parameter of the DomainQuery object"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got the list of annotations", response = Annotation.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting the list of Annotations")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("annotations/query")
    public List<Annotation> getAnnotations(@ApiParam QueryAnnotationParams params) {
        LOG.trace("Start getAnnotations({})", params);
        try {
            if (CollectionUtils.isEmpty(params.getReferences())) {
                return Collections.emptyList();
            } else {
                return annotationDao.findAnnotationsByTargetsAccessibleBySubjectKey(params.getReferences(), params.getSubjectKey());
            }
        } catch (Exception e) {
            LOG.error("Error occurred getting annotations using {}", params, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished getAnnotations({})", params);
        }
    }

    @ApiOperation(value = "Removes an Annotation",
            notes = "Removes an annotation using the Annotation Id"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully removed the annotation"),
            @ApiResponse(code = 500, message = "Internal Server Error removing the annotation")
    })
    @PUT
    @Path("annotations/remove")
    public Response removeAnnotations(@ApiParam RemoveAnnotationParams params) {
        LOG.trace("Start removeAnnotations({})", params);
        try {
            Annotation annotation = annotationDao.findEntityByIdReadableBySubjectKey(params.getAnnotationId(), params.getSubjectKey());
            if (annotation==null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Could not find annotation with id " + params.getAnnotationId()))
                        .build();
            }
            annotationDao.deleteByIdAndSubjectKey(params.getAnnotationId(), params.getSubjectKey());
            LOG.info("{} deleted annotation {} on {}: {}={}", params.getSubjectKey(), annotation.getId(), annotation.getTarget(), annotation.getKey(), annotation.getValue());
            return Response.noContent().build();
        } finally {
            LOG.trace("Finished removeAnnotations({})", params);
        }
    }

}

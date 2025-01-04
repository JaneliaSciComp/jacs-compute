package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.Collections;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.domain.dao.AnnotationDao;
import org.janelia.model.domain.dto.annotation.CreateAnnotationParams;
import org.janelia.model.domain.dto.annotation.QueryAnnotationParams;
import org.janelia.model.domain.dto.annotation.RemoveAnnotationParams;
import org.janelia.model.domain.dto.annotation.UpdateAnnotationParams;
import org.janelia.model.domain.ontology.Annotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "Annotations", description = "Janelia Workstation Domain Data")
@RequireAuthentication
@Path("/data")
public class AnnotationResource {
    private static final Logger LOG = LoggerFactory.getLogger(AnnotationResource.class);

    @AsyncIndex
    @Inject
    private AnnotationDao annotationDao;

    @Operation(summary = "Creates an annotation",
            description = "Creates a new annotation based on the given parameters"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully created annotation"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error creating Annotation")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("annotations")
    public Annotation createAnnotation(CreateAnnotationParams params) {
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

    @Operation(summary = "Updates an annotation",
            description = "Updates the value of a given annotation"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated annotation"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error updated Annotation")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("annotations/value")
    public Annotation updateAnnotation(UpdateAnnotationParams params) {
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

    @Operation(summary = "Returns a list of Annotations",
            description = "Returns a list of Annotations for the objects given in the references parameter of the DomainQuery object"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got the list of annotations"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting the list of Annotations")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("annotations/query")
    public List<Annotation> getAnnotations(QueryAnnotationParams params) {
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

    @Operation(summary = "Removes an Annotation",
            description = "Removes an annotation using the Annotation Id"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully removed the annotation"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error removing the annotation")
    })
    @PUT
    @Path("annotations/remove")
    public Response removeAnnotations(RemoveAnnotationParams params) {
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

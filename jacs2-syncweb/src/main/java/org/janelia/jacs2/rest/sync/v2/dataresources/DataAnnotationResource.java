package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import org.apache.commons.lang.StringUtils;
import org.glassfish.jersey.server.ContainerRequest;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.AnnotationDao;
import org.janelia.model.access.domain.dao.OntologyDao;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.ontology.Annotation;
import org.janelia.model.domain.ontology.Ontology;
import org.janelia.model.domain.ontology.OntologyTerm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.ArrayList;
import java.util.List;

@SwaggerDefinition(
        securityDefinition = @SecurityDefinition(
                apiKeyAuthDefinitions = {
                        @ApiKeyAuthDefinition(key = "user", name = "username", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER),
                        @ApiKeyAuthDefinition(key = "runAs", name = "runasuser", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER)
                }
        )
)
@Api(value = "Janelia Workstation Domain Data")
@RequireAuthentication
@Path("/data")
public class DataAnnotationResource {
    private static final Logger LOG = LoggerFactory.getLogger(DataAnnotationResource.class);

    @Inject
    private AnnotationDao annotationDao;
    @Inject
    private LegacyDomainDao legacyDomainDao;

    @ApiOperation(value = "Creates an annotation",
            notes = "creates a new annotation from the DomainObject parameter and assigns ownership to the SubjectKey parameter"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully created annotation", response = Annotation.class),
            @ApiResponse(code = 500, message = "Internal Server Error creating Annotation")
    })
    @PUT
    @Path("annotation")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Annotation createAnnotation(@ApiParam DomainQuery query) {
        LOG.trace("Start createAnnotation({})", query);
        try {
            return legacyDomainDao.save(query.getSubjectKey(), query.getDomainObjectAs(Annotation.class));
        } catch (Exception e) {
            LOG.error("Error occurred creating annotations for {}", query, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished createAnnotation({})", query);
        }
    }

    @POST
    @Path("annotation")
    @ApiOperation(value = "Updates an annotation",
            notes = "updates an existing annotation using the DomainObject parameter"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated annotation", response = Annotation.class),
            @ApiResponse(code = 500, message = "Internal Server Error updated Annotation")
    })
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Annotation updateAnnotation(@ApiParam DomainQuery query) {
        LOG.trace("Start updateAnnotation({})", query);
        try {
            return legacyDomainDao.save(query.getSubjectKey(), query.getDomainObjectAs(Annotation.class));
        } catch (Exception e) {
            LOG.error("Error occurred updating annotations {}", query, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished updateAnnotation({})", query);
        }
    }

    @POST
    @Path("annotation/details")
    @ApiOperation(value = "gets a list of Annotations",
            notes = "Gets a list of Annotations using the references paramter of the DomainQuery object"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got the list of annotations", response = Annotation.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting the list of Annotations")
    })
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public List<Annotation> getAnnotations(@ApiParam DomainQuery query) {
        LOG.trace("Start getAnnotations({})", query);
        try {
            return legacyDomainDao.getAnnotations(query.getSubjectKey(), query.getReferences());
        } catch (Exception e) {
            LOG.error("Error occurred getting annotations using {}", query, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished getAnnotations({})", query);
        }
    }

    @ApiOperation(value = "Removes an Annotation",
            notes = "Removes an annotation using the Annotation Id"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully removed the annotation"),
            @ApiResponse(code = 500, message = "Internal Server Error removing the annotation")
    })
    @DELETE
    @Path("annotation")
    public Response removeAnnotations(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                  @ApiParam @QueryParam("annotationId") final String annotationIdParam) {
        LOG.trace("Start removeAnnotations({},{})", subjectKey, annotationIdParam);
        Long annotationId;
        try {
            annotationId = Long.valueOf(annotationIdParam);
        } catch (Exception e) {
            LOG.error("Invalid annotation ID: {}", annotationIdParam, e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("Invalid annotationId " + annotationIdParam))
                    .build();
        }
        try {
            annotationDao.deleteByIdAndSubjectKey(annotationId, subjectKey);
            return Response.noContent().build();
        } finally {
            LOG.trace("Finished removeAnnotations({},{})", subjectKey, annotationIdParam);
        }
    }

}

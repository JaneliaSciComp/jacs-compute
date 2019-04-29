package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import org.apache.commons.lang.StringUtils;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.domain.dao.OntologyDao;
import org.janelia.model.domain.dto.DomainQuery;
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
import javax.ws.rs.container.ContainerRequestContext;
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
@Api(
        value = "Janelia Workstation Domain Data",
        authorizations = {
                @Authorization("user"),
                @Authorization("runAs")
        }
)
@RequireAuthentication
@Path("/data")
public class OntologyResource {
    private static final Logger LOG = LoggerFactory.getLogger(OntologyResource.class);

    @Inject
    private OntologyDao ontologyDao;

    @ApiOperation(value = "Gets all the ontologies available to a user")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully returned Ontologies", response=Ontology.class,
                    responseContainer = "List"),
            @ApiResponse( code = 500, message = "Internal Server Error getting list of Ontologies" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ontologies/{id}")
    public Response getOntologyById(@ApiParam @PathParam("id") final Long ontologyId,
                                    @Context ContainerRequestContext containerRequestContext) {
        LOG.trace("Start getOntologyById({})", ontologyId);
        try {
            String authorizedSubjectKey = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
            Ontology ontology = ontologyDao.findEntityByIdAccessibleBySubjectKey(ontologyId, authorizedSubjectKey);
            if (ontology != null) {
                return Response.ok()
                        .entity(ontology)
                        .build();
            } else {
                LOG.warn("No ontology found for {} accessible by {}", ontologyId, authorizedSubjectKey);
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No ontology found for " + ontologyId + " accessible by " + authorizedSubjectKey))
                        .build();
            }
        } finally {
            LOG.trace("Finished getOntologyById({})", ontologyId);
        }
    }

    @ApiOperation(value = "Gets all the ontologies available to a user")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully returned Ontologies", response=Ontology.class,
                    responseContainer = "List"),
            @ApiResponse( code = 500, message = "Internal Server Error getting list of Ontologies" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ontology")
    public Response getSubjectOntologies(@ApiParam @QueryParam("subjectKey") final String subjectKey) {
        LOG.trace("Start getSubjectOntologies({})", subjectKey);
        try {
            List<Ontology> accessibleOntologies = ontologyDao.getOntologiesAccessibleBySubjectGroups(subjectKey, 0, -1);
            return Response.ok()
                    .entity(new GenericEntity<List<Ontology>>(accessibleOntologies){})
                    .build();
        } finally {
            LOG.trace("Finished getSubjectOntologies({})", subjectKey);
        }
    }

    @ApiOperation(value = "Creates An Ontology",
            notes = "Uses the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully created an Ontology", response = Ontology.class),
            @ApiResponse(code = 500, message = "Internal Server Error creating an Ontology")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ontology")
    public Response createOntology(@ApiParam DomainQuery query) {
        LOG.trace("Start createOntology({})", query);
        try {
            Ontology ontology = ontologyDao.saveBySubjectKey(query.getDomainObjectAs(Ontology.class), query.getSubjectKey());
            return Response.created(UriBuilder.fromMethod(OntologyResource.class, "getOntologyById").build(ontology.getId()))
                    .entity(ontology)
                    .build();
        } finally {
            LOG.trace("Finished createOntology({})", query);
        }
    }

    @ApiOperation(value = "Removes An Ontology",
            notes = "Uses the ontologyId to remove the ontology"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully removed an Ontology"),
            @ApiResponse( code = 500, message = "Internal Server Error removing an ontology" )
    })
    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ontology")
    public Response removeOntology(@ApiParam @QueryParam("subjectKey") final String subjectKeyParam,
                                   @ApiParam @QueryParam("ontologyId") final String ontologyIdParam,
                                   @Context ContainerRequestContext containerRequestContext) {
        LOG.trace("Start removeOntology({}, {})", subjectKeyParam, ontologyIdParam);
        Long ontologyId;
        try {
            ontologyId = Long.valueOf(ontologyIdParam);
        } catch (Exception e) {
            LOG.error("Invalid ontology ID {}", ontologyIdParam, e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("Invalid ontology ID " + ontologyIdParam))
                    .build();
        }
        try {
            String authorizedSubjectKey = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
            String subjectKey = StringUtils.defaultIfBlank(subjectKeyParam, authorizedSubjectKey);
            ontologyDao.deleteByIdAndSubjectKey(ontologyId, subjectKey);
            LOG.info("Ontology {} was removed by {}", ontologyId, subjectKey);
            return Response.noContent().build();
        } finally {
            LOG.trace("Finished removeOntology({}, {})", subjectKeyParam, ontologyIdParam);
        }
    }

    @ApiOperation(value = "Adds Terms to an Ontology",
            notes = "Uses the ObjectId parameter of the DomainQuery (1st object is the ontology id," +
                    " second object is the parent id) and serialized JSON list of OntologyTerm as the " +
                    "ObjectList parameter."
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully added terms to Ontology", response=Ontology.class),
            @ApiResponse( code = 500, message = "Internal Server Error adding terms to an Ontology" )
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ontology/terms")
    public Response addTermsToOntology(@ApiParam DomainQuery query) {
        LOG.trace("Start addTermsToOntology({})", query);
        try {
            List<Long> objectIds = query.getObjectIds();
            Long ontologyId = objectIds.get(0); // first object from the list is the ontology ID
            Long parentId = objectIds.get(1); // second object from the list is the parent term ID
            List<OntologyTerm> terms = new ArrayList<>();
            for (OntologyTerm term : query.getObjectList()) {
                terms.add(term);
            }
            Ontology updatedOntology = ontologyDao.addTerms(
                    query.getSubjectKey(),
                    ontologyId,
                    parentId,
                    terms,
                    query.getOrdering().get(0)
            );
            if (updatedOntology == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Ontology " + ontologyId + " not found or not updateable by " + query.getSubjectKey()))
                        .build();
            } else {
                return Response.ok()
                        .entity(updatedOntology)
                        .build();
            }
        } finally {
            LOG.trace("Finished addTermsToOntology({})", query);
        }
    }

    @ApiOperation(value = "Reorders Terms in an Ontology",
            notes = "Uses the ObjectId parameter of the DomainQuery (1st object is the ontology id," +
                    " second object is the parent id) and for ordering the Ordering parameter of the DomainQuery."
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully reordered terms in Ontology", response=Ontology.class),
            @ApiResponse( code = 500, message = "Internal Server Error reordered terms in Ontology" )
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ontology/terms")
    public Response reorderOntology(@ApiParam DomainQuery query) {
        LOG.trace("Start reorderOntology({})", query);
        try {
            List<Long> objectIds = query.getObjectIds();
            Long ontologyId = objectIds.get(0);
            Long parentId = objectIds.get(1);
            int[] order = new int[query.getOrdering().size()];
            for (int i = 0; i < order.length; i++) {
                order[i] = query.getOrdering().get(i);
            }
            Ontology updatedOntology = ontologyDao.reorderTerms(
                    query.getSubjectKey(),
                    ontologyId,
                    parentId,
                    order
            );
            if (updatedOntology == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Ontology " + ontologyId + " not found or not updateable by " + query.getSubjectKey()))
                        .build();
            } else {
                return Response.ok()
                        .entity(updatedOntology)
                        .build();
            }
        } finally {
            LOG.trace("Finished reorderOntology({})", query);
        }
    }

    @ApiOperation(value = "Removes Terms from an Ontology",
            notes = "Uses the ontologyId, parentTermId to find the Ontology. " +
                    "The termId is the id in the ontology to remove."
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully removed a term from Ontology", response=Ontology.class),
            @ApiResponse( code = 500, message = "Internal Server Error removed term from Ontology" )
    })
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ontology/terms")
    public Response removeTermsFromOntology(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                            @ApiParam @QueryParam("ontologyId") final Long ontologyId,
                                            @ApiParam @QueryParam("parentTermId") final Long parentTermId,
                                            @ApiParam @QueryParam("termId") final Long termId) {
        LOG.trace("Start removeTermsFromOntology({}, ontologyId={}, parentTermId={}, termId={})", subjectKey, ontologyId, parentTermId, termId);
        try {
            Ontology updatedOntology = ontologyDao.removeTerm(
                    subjectKey,
                    ontologyId,
                    parentTermId,
                    termId
            );
            if (updatedOntology == null) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Ontology " + ontologyId + " not found or not updateable by " + subjectKey))
                        .build();
            } else {
                return Response.ok()
                        .entity(updatedOntology)
                        .build();
            }
        } finally {
            LOG.trace("Finished removeTermsFromOntology({}, ontologyId={}, parentTermId={}, termId={})", subjectKey, ontologyId, parentTermId, termId);
        }
    }

}

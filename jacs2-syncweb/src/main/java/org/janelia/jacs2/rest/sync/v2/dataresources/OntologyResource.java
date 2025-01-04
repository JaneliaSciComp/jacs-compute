package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.GenericEntity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.domain.dao.OntologyDao;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.ontology.Ontology;
import org.janelia.model.domain.ontology.OntologyTerm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "Ontology", description = "Janelia Workstation Domain Data")
@RequireAuthentication
@Path("/data")
public class OntologyResource {
    private static final Logger LOG = LoggerFactory.getLogger(OntologyResource.class);

    @AsyncIndex
    @Inject
    private OntologyDao ontologyDao;

    @Operation(summary = "Gets all the ontologies available to a user")
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully returned Ontologies"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error getting list of Ontologies" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ontologies/{id}")
    public Response getOntologyById(@Parameter @PathParam("id") final Long ontologyId,
                                    @Context ContainerRequestContext containerRequestContext) {
        LOG.trace("Start getOntologyById({})", ontologyId);
        try {
            String authorizedSubjectKey = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
            Ontology ontology = ontologyDao.findEntityByIdReadableBySubjectKey(ontologyId, authorizedSubjectKey);
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

    @Operation(summary = "Gets all the ontologies available to a user")
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully returned Ontologies"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error getting list of Ontologies" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ontology")
    public Response getSubjectOntologies(@Parameter @QueryParam("subjectKey") final String subjectKey) {
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

    @Operation(summary = "Creates An Ontology",
            description = "Uses the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully created an Ontology"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error creating an Ontology")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ontology")
    public Response createOntology(@Parameter DomainQuery query) {
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

    @Operation(summary = "Removes An Ontology",
            description = "Uses the ontologyId to remove the ontology"
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully removed an Ontology"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error removing an ontology" )
    })
    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ontology")
    public Response removeOntology(@Parameter @QueryParam("subjectKey") final String subjectKeyParam,
                                   @Parameter @QueryParam("ontologyId") final String ontologyIdParam,
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

    @Operation(summary = "Adds Terms to an Ontology",
            description = "Uses the ObjectId parameter of the DomainQuery (1st object is the ontology id," +
                    " second object is the parent id) and serialized JSON list of OntologyTerm as the " +
                    "ObjectList parameter."
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully added terms to Ontology"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error adding terms to an Ontology" )
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ontology/terms")
    public Response addTermsToOntology(@Parameter DomainQuery query) {
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

    @Operation(summary = "Reorders Terms in an Ontology",
            description = "Uses the ObjectId parameter of the DomainQuery (1st object is the ontology id," +
                    " second object is the parent id) and for ordering the Ordering parameter of the DomainQuery."
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully reordered terms in Ontology"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error reordered terms in Ontology" )
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ontology/terms")
    public Response reorderOntology(@Parameter DomainQuery query) {
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

    @Operation(summary = "Removes Terms from an Ontology",
            description = "Uses the ontologyId, parentTermId to find the Ontology. " +
                    "The termId is the id in the ontology to remove."
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully removed a term from Ontology"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error removed term from Ontology" )
    })
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/ontology/terms")
    public Response removeTermsFromOntology(@Parameter @QueryParam("subjectKey") final String subjectKey,
                                            @Parameter @QueryParam("ontologyId") final Long ontologyId,
                                            @Parameter @QueryParam("parentTermId") final Long parentTermId,
                                            @Parameter @QueryParam("termId") final Long termId) {
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

package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.DatasetDao;
import org.janelia.model.access.domain.dao.OntologyDao;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.ontology.Ontology;
import org.janelia.model.domain.sample.DataSet;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriBuilder;
import java.math.BigDecimal;
import java.util.List;

@Api(value = "Janelia Workstation Ontology Data")
@Path("/data")
public class AnnotationDataResource {
    private static final Logger LOG = LoggerFactory.getLogger(AnnotationDataResource.class);

    @Inject
    private OntologyDao ontologyDao;

    @ApiOperation(value = "Gets all the ontologies for a user")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully returned Ontologies", response=Ontology.class,
                    responseContainer = "List"),
            @ApiResponse( code = 500, message = "Internal Server Error getting list of Ontologies" )
    })
    @GET
    @Path("/ontology")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSubjectOntologies(@ApiParam @QueryParam("subjectKey") final String subjectKey) {
        LOG.trace("Start getSubjectOntologies({})", subjectKey);
        try {
            List<Ontology> subjectOntologies = ontologyDao.getAllOntologiesByOwnerKey(subjectKey, 0, -1);
            return Response.ok()
                    .entity(new GenericEntity<List<Ontology>>(subjectOntologies){})
                    .build();
        } finally {
            LOG.trace("Finished getSubjectOntologies({})", subjectKey);
        }
    }

}

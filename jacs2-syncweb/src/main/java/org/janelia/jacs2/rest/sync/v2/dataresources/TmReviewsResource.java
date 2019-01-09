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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.BodyPartEntity;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartMediaTypes;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.access.domain.dao.TmNeuronMetadataDao;
import org.janelia.model.access.domain.dao.TmReviewTaskDao;
import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.tiledMicroscope.BulkNeuronStyleUpdate;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmReviewTask;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
import org.janelia.model.domain.workspace.Workspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@SwaggerDefinition(
        securityDefinition = @SecurityDefinition(
                apiKeyAuthDefinitions = {
                        @ApiKeyAuthDefinition(key = "user", name = "username", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER),
                        @ApiKeyAuthDefinition(key = "runAs", name = "runasuser", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER)
                }
        )
)
@Api(
        value = "Janelia Mouselight Data Service",
        authorizations = {
                @Authorization("user"),
                @Authorization("runAs")
        }
)
@RequireAuthentication
@ApplicationScoped
@Produces("application/json")
@Path("/mouselight/data")
public class TmReviewsResource {
    private static final Logger LOG = LoggerFactory.getLogger(TmReviewsResource.class);

    @Inject
    private TmReviewTaskDao tmReviewTaskDao;

    @GET
    @Path("/reviewtask")
    @ApiOperation(value = "Gets all review tasks",
            notes = "Returns a list of all the review tasks currently in the system"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the list of review tasks",  response = TmReviewTask.class,
                    responseContainer = "List" ),
            @ApiResponse( code = 500, message = "Error occurred while fetching the review tasks" )
    })
    @Produces(MediaType.APPLICATION_JSON)
    public List<TmReviewTask> getTmReviewTasks(@ApiParam @QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getTmReviewTasks()");
        return tmReviewTaskDao.getReviewTasksForSubject(subjectKey);
    }

    @PUT
    @Path("/reviewtask")
    @ApiOperation(value = "Creates a new TmReviewTask",
            notes = "Creates a TmWorkspace using the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully created a TmReviewTask", response = TmReviewTask.class),
            @ApiResponse( code = 500, message = "Error occurred while creating a TmReviewTask" )
    })
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public TmReviewTask createTmReviewTask(DomainQuery query) {
        LOG.trace("Start createTmReviewTask({})", query);
        return tmReviewTaskDao.createTmReviewTask(query.getSubjectKey(), (TmReviewTask) query.getDomainObject());
    }

    @POST
    @Path("/reviewtask")
    @ApiOperation(value = "Updates an existing TmReviewTask",
            notes = "Updates a TmReviewTask using the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully updated a TmReviewTask", response = TmReviewTask.class),
            @ApiResponse( code = 500, message = "Error occurred while updating a TmReviewTask" )
    })
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public TmReviewTask updateTmReviewTask(@ApiParam DomainQuery query) {
        LOG.trace("Start updateTmReviewTask({})", query);
        return tmReviewTaskDao.updateTmReviewTask(query.getSubjectKey(), (TmReviewTask) query.getDomainObject());
    }

    @DELETE
    @Path("/reviewtask")
    @ApiOperation(value = "Removes a TmReviewTask",
            notes = "Removes the TmReviewTask using the TmReviewTask Id"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully removed a TmReviewTask"),
            @ApiResponse( code = 500, message = "Error occurred while removing a TmReviewTask" )
    })
    public void removeTmReviewTask(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                   @ApiParam @QueryParam("taskReviewId") final Long taskReviewId) {
        LOG.trace("Start removeTmReviewTask({}, taskReviewId={})", subjectKey, taskReviewId);
        tmReviewTaskDao.deleteByIdAndSubjectKey(taskReviewId, subjectKey);
    }

}

package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.domain.dao.TmReviewTaskDao;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.tiledMicroscope.TmReviewTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "TmReview", description = "Janelia Mouselight Data Service")
@RequireAuthentication
@ApplicationScoped
@Produces("application/json")
@Path("/mouselight/data")
public class TmReviewsResource {
    private static final Logger LOG = LoggerFactory.getLogger(TmReviewsResource.class);

    @AsyncIndex
    @Inject
    private TmReviewTaskDao tmReviewTaskDao;

    @Operation(summary = "Gets all review tasks",
            description = "Returns a list of all the review tasks currently in the system"
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully fetched the list of review tasks"),
            @ApiResponse( responseCode = "500", description = "Error occurred while fetching the review tasks" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/reviewtask")
    public List<TmReviewTask> getTmReviewTasks(@QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getTmReviewTasks()");
        return tmReviewTaskDao.getReviewTasksForSubject(subjectKey);
    }

    @Operation(summary = "Creates a new TmReviewTask",
            description = "Creates a TmWorkspace using the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully created a TmReviewTask"),
            @ApiResponse( responseCode = "500", description = "Error occurred while creating a TmReviewTask")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/reviewtask")
    public TmReviewTask createTmReviewTask(DomainQuery query) {
        LOG.trace("Start createTmReviewTask({})", query);
        return tmReviewTaskDao.createTmReviewTask(query.getSubjectKey(), (TmReviewTask) query.getDomainObject());
    }

    @POST
    @Path("/reviewtask")
    @Operation(summary = "Updates an existing TmReviewTask",
            description = "Updates a TmReviewTask using the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully updated a TmReviewTask"),
            @ApiResponse( responseCode = "500", description = "Error occurred while updating a TmReviewTask" )
    })
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public TmReviewTask updateTmReviewTask(DomainQuery query) {
        LOG.trace("Start updateTmReviewTask({})", query);
        return tmReviewTaskDao.updateTmReviewTask(query.getSubjectKey(), (TmReviewTask) query.getDomainObject());
    }

    @DELETE
    @Path("/reviewtask")
    @Operation(summary = "Removes a TmReviewTask",
            description = "Removes the TmReviewTask using the TmReviewTask Id"
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully removed a TmReviewTask"),
            @ApiResponse( responseCode = "500", description = "Error occurred while removing a TmReviewTask" )
    })
    public void removeTmReviewTask(@QueryParam("subjectKey") final String subjectKey,
                                   @QueryParam("taskReviewId") final Long taskReviewId) {
        LOG.trace("Start removeTmReviewTask({}, taskReviewId={})", subjectKey, taskReviewId);
        tmReviewTaskDao.deleteByIdAndSubjectKey(taskReviewId, subjectKey);
    }

}

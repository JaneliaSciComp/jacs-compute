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
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.GenericEntity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.UriBuilder;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.LineReleaseDao;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.sample.LineRelease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "LineRelease", description = "Janelia Workstation Line Release(s)")
@RequireAuthentication
@Path("/process")
public class LineReleaseResource {
    private static final Logger LOG = LoggerFactory.getLogger(LineReleaseResource.class);
    private static final String FLYLIGHT_TECHNICAL = "group:flylighttechnical";

    @AsyncIndex
    @Inject
    private LineReleaseDao lineReleaseDao;
    @Inject
    private LegacyDomainDao legacyDomainDao;

    @Operation(summary = "Gets Release Information for a specific user")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got release information"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting Release Information")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("release")
    public Response getReleasesInfo(@Parameter @QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getReleasesInfo({})", subjectKey);
        try {
            List<LineRelease> lineReleases = legacyDomainDao.getDomainObjects(subjectKey, LineRelease.class);
            return Response
                    .ok(new GenericEntity<List<LineRelease>>(lineReleases){})
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred getting releases info for {}", subjectKey, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving line releases"))
                    .build();
        } finally {
            LOG.trace("Finished getReleasesInfo({})", subjectKey);
        }
    }

    @Operation(summary = "Gets Release Information about a specific release")
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully got release information"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error getting Release Information" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("release/releaseId/{releaseId}")
    public Response getReleaseInfoById(@Parameter @PathParam("releaseId") Long releaseId,
                                       @Context SecurityContext securityContext) {
        LOG.trace("Start getReleaseInfoById({}) by {}", releaseId, securityContext.getUserPrincipal());
        try {
            LineRelease lineRelease = lineReleaseDao.findEntityByIdReadableBySubjectKey(releaseId, securityContext.getUserPrincipal().getName());
            if (lineRelease != null) {
                return Response
                        .ok(lineRelease)
                        .build();
            } else {
                LineRelease existingRelease = lineReleaseDao.findById(releaseId); // this is only for logging purposes
                if (existingRelease != null) {
                    LOG.info("A line release exists for {} but is not accessible by {}", releaseId, securityContext.getUserPrincipal());
                    return Response.status(Response.Status.FORBIDDEN)
                            .entity(new ErrorResponse("Line release is not accessible"))
                            .build();
                } else {
                    LOG.info("No line release found for {}", releaseId);
                    return Response.status(Response.Status.NOT_FOUND)
                            .entity(new ErrorResponse("Line release does not exist"))
                            .build();
                }
            }
        } finally {
            LOG.trace("Finished getReleaseInfoById({}) by {}", releaseId, securityContext.getUserPrincipal());
        }
    }

    @Operation(summary = "Gets Release Information about a specific release for a specific user")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got release information"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting Release Information")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("release/{releaseName}")
    public Response getReleaseInfo(@Parameter @QueryParam("subjectKey") String subjectKey,
                                   @Parameter @PathParam("releaseName") String releaseName) {
        LOG.trace("Start getReleaseInfo({}, {})", subjectKey, releaseName);
        try {
            List<LineRelease> lineReleases = legacyDomainDao.getDomainObjectsByName(subjectKey, LineRelease.class, releaseName);
            return Response
                    .ok(new GenericEntity<List<LineRelease>>(lineReleases){})
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred getting releases info about {} for {}", releaseName, subjectKey, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error retrieving release info about " + releaseName))
                    .build();
        } finally {
            LOG.trace("Finished getReleaseInfo({}, {})", subjectKey, releaseName);
        }
    }

    @Operation(summary = "Creates a Line Release using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully created a new release"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error creating a release")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("release")
    public Response createRelease(DomainQuery query) {
        LOG.trace("Start createRelease({})", query);
        try {
            LineRelease lineReleaseArg = query.getDomainObjectAs(LineRelease.class);
            if (lineReleaseArg.getTargetWebsite()==null) {
                lineReleaseArg.setTargetWebsite(LineRelease.TARGET_WEBSITES[0]);
            }
            // JW-45968: Share every release with FlyLight technicians by default
            lineReleaseArg.getReaders().add(FLYLIGHT_TECHNICAL);
            lineReleaseArg.getWriters().add(FLYLIGHT_TECHNICAL);
            LineRelease lineRelease = lineReleaseDao.saveBySubjectKey(lineReleaseArg, query.getSubjectKey());

            return Response
                    .created(UriBuilder.fromMethod(this.getClass(), "getReleaseInfoById").build(lineRelease.getId()))
                    .entity(lineRelease)
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred creating the line release {}", query, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error while updating the line release " + query))
                    .build();
        } finally {
            LOG.trace("Finished createRelease({})", query);
        }
    }

    @Operation(summary = "Updates a Line Release using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated a release"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error updating a release")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("release")
    public Response updateRelease(DomainQuery query) {
        LOG.trace("Start updateRelease({})", query);
        try {
            LineRelease lineReleaseArg = query.getDomainObjectAs(LineRelease.class);
            if (lineReleaseArg.getId()==null) {
                LOG.error("Line release not found for update: {}", query);
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Release not found: " + lineReleaseArg.getId()))
                        .build();
            }
            LineRelease lineRelease = lineReleaseDao.saveBySubjectKey(lineReleaseArg, query.getSubjectKey());
            return Response
                    .ok(lineRelease)
                    .contentLocation(UriBuilder.fromMethod(LineReleaseResource.class, "getReleaseInfoById").build(lineRelease.getId()))
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred updating the line release {}", query, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error while updating the release " + query))
                    .build();
        } finally {
            LOG.trace("Finished updateRelease({})", query);
        }
    }

    @Operation(summary = "Removes the Line Release using the release Id")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully removed a release"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error removing a release")
    })
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("release")
    public Response removeRelease(@Parameter @QueryParam("subjectKey") String subjectKey,
                                  @Parameter @QueryParam("releaseId") String releaseIdParam) {
        LOG.trace("Start removeRelease({}, releaseId={})", subjectKey, releaseIdParam);
        Long releaseId;
        try {
            try {
                releaseId = Long.valueOf(releaseIdParam);
            } catch (Exception e) {
                LOG.error("Invalid release ID: {}", releaseIdParam, e);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("Invalid release ID"))
                        .build();
            }
            lineReleaseDao.deleteByIdAndSubjectKey(releaseId, subjectKey);
            return Response.noContent()
                    .build();
        } finally {
            LOG.trace("Finished removeRelease({}, releaseId={})", subjectKey, releaseIdParam);
        }
    }

}

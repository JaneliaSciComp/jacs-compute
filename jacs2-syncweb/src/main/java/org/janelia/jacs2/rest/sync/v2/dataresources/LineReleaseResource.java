package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.List;

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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriBuilder;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.LineReleaseDao;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.sample.LineRelease;
import org.janelia.model.security.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SwaggerDefinition(
        securityDefinition = @SecurityDefinition(
                apiKeyAuthDefinitions = {
                        @ApiKeyAuthDefinition(key = "user", name = "username", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER),
                        @ApiKeyAuthDefinition(key = "runAs", name = "runasuser", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER)
                }
        )
)
@Api(
        value = "Janelia Workstation Line Release(s)",
        authorizations = {
                @Authorization("user"),
                @Authorization("runAs")
        }
)
@RequireAuthentication
@ApplicationScoped
@Path("/process")
public class LineReleaseResource {
    private static final Logger LOG = LoggerFactory.getLogger(LineReleaseResource.class);
    private static final String FLYLIGHT_TECHNICAL = "group:flylighttechnical";

    @AsyncIndex
    @Inject
    private LineReleaseDao lineReleaseDao;
    @Inject
    private LegacyDomainDao legacyDomainDao;

    @ApiOperation(value = "Gets Release Information for a specific user")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got release information",
                    response = Response.class, responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting Release Information")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("release")
    public Response getReleasesInfo(@ApiParam @QueryParam("subjectKey") String subjectKey) {
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

    @ApiOperation(value = "Gets Release Information about a specific release")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully got release information",
                    response = Response.class, responseContainer = "List"),
            @ApiResponse( code = 500, message = "Internal Server Error getting Release Information" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("release/releaseId/{releaseId}")
    public Response getReleaseInfoById(@ApiParam @PathParam("releaseId") Long releaseId,
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

    @ApiOperation(value = "Gets Release Information about a specific release for a specific user")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got release information",
                    response = Response.class, responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting Release Information")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("release/{releaseName}")
    public Response getReleaseInfo(@ApiParam @QueryParam("subjectKey") String subjectKey,
                                   @ApiParam @PathParam("releaseName") String releaseName) {
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

    @ApiOperation(value = "Creates a Line Release using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully created a new release", response = LineRelease.class),
            @ApiResponse(code = 500, message = "Internal Server Error creating a release")
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

    @ApiOperation(value = "Updates a Line Release using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated a release", response = Response.class),
            @ApiResponse(code = 500, message = "Internal Server Error updating a release")
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

    @ApiOperation(value = "Removes the Line Release using the release Id")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully removed a release"),
            @ApiResponse(code = 500, message = "Internal Server Error removing a release")
    })
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("release")
    public Response removeRelease(@ApiParam @QueryParam("subjectKey") String subjectKey,
                                  @ApiParam @QueryParam("releaseId") String releaseIdParam) {
        LOG.trace("Start removeRelease({}, releaseId={})", subjectKey, releaseIdParam);
        Long releaseId;
        try {
            try {
                releaseId = new Long(releaseIdParam);
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

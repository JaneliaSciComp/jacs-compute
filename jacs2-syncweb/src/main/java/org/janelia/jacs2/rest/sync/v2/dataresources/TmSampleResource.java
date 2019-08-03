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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.dataservice.rendering.RenderedVolumeLocationFactory;
import org.janelia.jacs2.dataservice.search.IndexingService;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.TmSampleDao;
import org.janelia.model.domain.DomainConstants;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.rendering.RenderedVolumeLocation;
import org.janelia.rendering.StreamableContent;
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
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Web service for CRUD operations having to do with Tiled Microscope domain objects.
 */
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
public class TmSampleResource {

    private static final Logger LOG = LoggerFactory.getLogger(TmSampleResource.class);

    @Inject
    private LegacyDomainDao legacyDomainDao;
    @AsyncIndex
    @Inject
    private TmSampleDao tmSampleDao;
    @Inject
    private RenderedVolumeLocationFactory renderedVolumeLocationFactory;

    @ApiOperation(value = "Gets a list of sample root paths",
            notes = "Returns a list of all the sample root paths used for LVV sample discovery"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched the list of sample paths", response = String.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Error occurred while fetching sample paths")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("sampleRootPaths")
    public Response getSampleRootPreferences(@ApiParam @QueryParam("subjectKey") final String subjectKey) {
        String updatedSubjectKey = StringUtils.defaultIfBlank(subjectKey, DomainConstants.MOUSELIGHT_GROUP_KEY);
        LOG.trace("Start getTmSamplePaths({})", updatedSubjectKey);
        try {
            @SuppressWarnings("unchecked")
            List<String> sampleRootPreferences = (List<String>) legacyDomainDao.getPreferenceValue(
                    updatedSubjectKey,
                    DomainConstants.PREFERENCE_CATEGORY_MOUSELIGHT,
                    DomainConstants.PREFERENCE_NAME_SAMPLE_ROOTS);
            return Response.ok()
                    .entity(sampleRootPreferences)
                    .build();
        } finally {
            LOG.trace("Finished getTmSamplePaths({})", updatedSubjectKey);
        }
    }

    @ApiOperation(value = "Updates the sample root paths",
            notes = "Updates the sample root paths used for LVV sample discovery"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated sample paths", response = TmSample.class),
            @ApiResponse(code = 500, message = "Error occurred while updating sample paths")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("sampleRootPaths")
    public Response updateSampleRootPreferences(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                                @ApiParam List<String> samplePaths) {
        String updatedSubjectKey = StringUtils.defaultIfBlank(subjectKey, DomainConstants.MOUSELIGHT_GROUP_KEY);
        LOG.trace("Start updateTmSamplePaths({}, {})", updatedSubjectKey, samplePaths);
        try {
            legacyDomainDao.setPreferenceValue(
                    updatedSubjectKey,
                    DomainConstants.PREFERENCE_CATEGORY_MOUSELIGHT,
                    DomainConstants.PREFERENCE_NAME_SAMPLE_ROOTS,
                    samplePaths);
            return Response.ok().build();
        } catch (Exception e) {
            LOG.error("Error updating sample root paths for {} to {}", updatedSubjectKey, samplePaths, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error while setting sample root paths for " + subjectKey + " to " + samplePaths))
                    .build();
        } finally {
            LOG.trace("Finish updateTmSamplePaths({}, {})", updatedSubjectKey, samplePaths);
        }
    }

    @ApiOperation(value = "Gets a list of TM Samples",
            notes = "Returns a list of all the TM Samples that are accessible by the current user"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched the list of samples", response = TmSample.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Error occurred while fetching the samples")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("sample")
    public List<TmSample> getTmSamples(@ApiParam @QueryParam("subjectKey") final String subjectKey) {
        String sampleOwnerKey = StringUtils.defaultIfBlank(subjectKey, DomainConstants.MOUSELIGHT_GROUP_KEY);
        return tmSampleDao.findOwnedEntitiesBySubjectKey(sampleOwnerKey, 0, -1);
    }

    @ApiOperation(
            value = "Gets a TM Sample by id",
            notes = "Returns the TM Sample identified by the given id"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched the sample", response = TmSample.class),
            @ApiResponse(code = 500, message = "Error occurred while fetching the sample")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("sample/{sampleId}")
    public TmSample getTmSample(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                @ApiParam @PathParam("sampleId") final Long sampleId) {
        return tmSampleDao.findEntityByIdAccessibleBySubjectKey(sampleId, subjectKey);
   }

    @ApiOperation(value = "Gets a calculated origin by Sample path",
            notes = "Returns a map of useful constants when creating a sample (origin, scaling)"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched the sample path constants", response = Map.class),
            @ApiResponse(code = 500, message = "Error occurred while fetching the sample constants")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("sample/constants")
    public Response getTmSampleConstants(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                         @ApiParam @QueryParam("samplePath") final String samplePath,
                                         @Context ContainerRequestContext containerRequestContext) {
        LOG.trace("getTmSampleConstants(subjectKey: {}, samplePath: {})", subjectKey, samplePath);
        // read and process transform.txt file in Sample path
        // this is intended to be a one-time process and data returned will be stored in TmSample upon creation
        if (StringUtils.isBlank(samplePath)) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("Invalid sample sample path - the sample path cannot be empty"))
                    .build();
        }
        RenderedVolumeLocation rvl = renderedVolumeLocationFactory.getVolumeLocation(samplePath,
                JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext),
                null);
        return rvl.getTransformData()
                .map(streamableTransform -> {
                    try {
                        Map<String, Object> constants = new HashMap<>();
                        Map<String, Integer> origin = new HashMap<>();
                        Map<String, Double> scaling = new HashMap<>();
                        BufferedReader reader = new BufferedReader(new InputStreamReader(streamableTransform.getStream()));
                        String line;
                        Map<String, Double> values = new HashMap<>();
                        while ((line = reader.readLine()) != null) {
                            String[] keyVals = line.split(":");
                            if (keyVals.length == 2) {
                                values.put(keyVals[0], Double.parseDouble(keyVals[1].trim()));
                            }
                        }
                        origin.put("x", values.get("ox").intValue());
                        origin.put("y", values.get("oy").intValue());
                        origin.put("z", values.get("oz").intValue());
                        scaling.put("x", values.get("sx"));
                        scaling.put("y", values.get("sy"));
                        scaling.put("z", values.get("sz"));

                        constants.put("origin", origin);
                        constants.put("scaling", scaling);
                        constants.put("numberLevels", values.get("nl").longValue());
                        return Response.ok()
                                .entity(new GenericEntity<Map<String, Object>>(constants){})
                                .build();
                    } catch (Exception e) {
                        LOG.error("Error reading transform constants for {} from {}", subjectKey, samplePath, e);
                        return Response.status(Response.Status.NOT_FOUND)
                                .entity(new ErrorResponse("Error reading transform.txt from " + samplePath))
                                .build();
                    } finally {
                        IOUtils.closeQuietly(streamableTransform);
                    }
                })
                .orElseGet(() -> {
                    LOG.error("Transform constants file not found for {} from {}", subjectKey, samplePath);
                    return Response.status(Response.Status.NOT_FOUND)
                            .entity(new ErrorResponse("Missing transform.txt from " + samplePath))
                            .build();
                });
    }

    @ApiOperation(value = "Creates a new TmSample",
            notes = "Creates a TmSample using the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully created a TmSample", response = TmSample.class),
            @ApiResponse(code = 500, message = "Error occurred while creating a TmSample")
    })
    @PUT
    @Path("sample")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public TmSample createTmSample(DomainQuery query) {
        LOG.trace("createTmSample({})", query);
        return tmSampleDao.createTmSample(query.getSubjectKey(), query.getDomainObjectAs(TmSample.class));
    }

    @ApiOperation(value = "Updates an existing TmSample",
            notes = "Updates a TmSample using the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated a TmSample", response = TmSample.class),
            @ApiResponse(code = 500, message = "Error occurred while updating a TmSample")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("sample")
    public TmSample updateTmSample(@ApiParam DomainQuery query) {
        LOG.trace("updateTmSample({})", query);
        return tmSampleDao.updateTmSample(query.getSubjectKey(), query.getDomainObjectAs(TmSample.class));
    }

    @ApiOperation(value = "Removes a TmSample",
            notes = "Removes the TmSample using the TmSample Id"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully removed a TmSample"),
            @ApiResponse(code = 500, message = "Error occurred while removing a TmSample")
    })
    @DELETE
    @Path("sample")
    public void removeTmSample(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                               @ApiParam @QueryParam("sampleId") final Long sampleId) {
        LOG.trace("removeTmSample(subjectKey: {}, sampleId: {})", subjectKey, sampleId);
        tmSampleDao.removeTmSample(subjectKey, sampleId);
    }

}

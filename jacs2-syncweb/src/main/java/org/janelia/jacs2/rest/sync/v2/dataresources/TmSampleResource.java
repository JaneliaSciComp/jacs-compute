package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.TmSampleDao;
import org.janelia.model.access.domain.dao.mongo.TmSampleMongoDao;
import org.janelia.model.cdi.WithCache;
import org.janelia.model.domain.DomainConstants;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.janelia.model.domain.workspace.Workspace;
import org.janelia.model.rendering.RenderedVolumeLoader;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Web service for CRUD operations having to do with Tiled Microscope domain objects.
 */
@Api(value = "MouseLight Data Service")
@RequireAuthentication
@ApplicationScoped
@Produces("application/json")
@Path("/mouselight/data")
public class TmSampleResource {

    @Inject private LegacyDomainDao legacyDomainDao;
    @Inject private TmSampleDao tmSampleDao;
    @WithCache @Inject private RenderedVolumeLoader renderedVolumeLoader;
    @Inject private Logger logger;

    @ApiOperation(value = "Gets a list of sample root paths",
            notes = "Returns a list of all the sample root paths used for LVV sample discovery"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the list of sample paths",  response = String.class,
                    responseContainer = "List" ),
            @ApiResponse( code = 500, message = "Error occurred while fetching sample paths" )
    })
    @GET
    @Path("sampleRootPaths")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getSampleRootPreferences(@ApiParam @QueryParam("subjectKey") final String subjectKey) {
        String updatedSubjectKey = StringUtils.defaultIfBlank(subjectKey, DomainConstants.MOUSELIGHT_GROUP_KEY);
        logger.trace("Start getTmSamplePaths({})", updatedSubjectKey);
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
            logger.trace("Finished getTmSamplePaths({})", updatedSubjectKey);

        }
    }

    @ApiOperation(value = "Updates the sample root paths",
            notes = "Updates the sample root paths used for LVV sample discovery"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully updated sample paths", response = TmSample.class),
            @ApiResponse( code = 500, message = "Error occurred while updating sample paths" )
    })
    @POST
    @Path("sampleRootPaths")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateSampleRootPreferences(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                                @ApiParam List<String> samplePaths) {
        String updatedSubjectKey = StringUtils.defaultIfBlank(subjectKey, DomainConstants.MOUSELIGHT_GROUP_KEY);
        logger.trace("Start updateTmSamplePaths({}, {})", updatedSubjectKey, samplePaths);
        try {
            legacyDomainDao.setPreferenceValue(
                    updatedSubjectKey,
                    DomainConstants.PREFERENCE_CATEGORY_MOUSELIGHT,
                    DomainConstants.PREFERENCE_NAME_SAMPLE_ROOTS,
                    samplePaths);
            return Response.ok().build();
        } catch (Exception e) {
            logger.error("Error updating sample root paths for {} to {}", updatedSubjectKey, samplePaths, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error while setting sample root paths for " + subjectKey + " to " + samplePaths))
                    .build();
        } finally {
            logger.trace("Finish updateTmSamplePaths({}, {})", updatedSubjectKey, samplePaths);
        }
    }

    @ApiOperation(value = "Gets a list of TM Samples",
            notes = "Returns a list of all the TM Samples that are accessible by the current user"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the list of samples",  response = TmSample.class,
                    responseContainer = "List" ),
            @ApiResponse( code = 500, message = "Error occurred while fetching the samples" )
    })
    @GET
    @Path("sample")
    @Produces(MediaType.APPLICATION_JSON)
    public List<TmSample> getTmSamples(@ApiParam @QueryParam("subjectKey") final String subjectKey) {
        String sampleOwnerKey = StringUtils.defaultIfBlank(subjectKey, DomainConstants.MOUSELIGHT_GROUP_KEY);
        return tmSampleDao.findByOwnerKey(sampleOwnerKey);
    }

    @ApiOperation(
            value = "Gets a TM Sample by id",
            notes = "Returns the TM Sample identified by the given id"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the sample",  response = TmSample.class),
            @ApiResponse( code = 500, message = "Error occurred while fetching the sample" )
    })
    @GET
    @Path("sample/{sampleId}")
    @Produces(MediaType.APPLICATION_JSON)
    public TmSample getTmSample(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                @ApiParam @PathParam("sampleId") final Long sampleId) {
        return tmSampleDao.findByIdAndSubjectKey(sampleId, subjectKey);
    }

    @ApiOperation(value = "Get sample rendering info", notes = "Retrieve volume rendering info for the specified sample")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 404, message = "Sample not found or no rendering"),
            @ApiResponse(code = 500, message = "Error occurred") })
    @GET
    @Path("sample/{sampleId}/sampleRenderingInfo")
    @Produces({MediaType.APPLICATION_JSON})
    public Response streamTileFromCoord(@PathParam("sampleId") Long sampleId) {
        TmSample tmSample = tmSampleDao.findById(sampleId);
        if (tmSample == null) {
            logger.warn("No sample found for {}", sampleId);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No sample found for " + sampleId))
                    .build();
        } else if (StringUtils.isBlank(tmSample.getFilepath())) {
            logger.warn("Sample {} found but it has not rendering path", tmSample);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No rendering path set for " + sampleId))
                    .build();
        }
        return renderedVolumeLoader.loadVolume(Paths.get(tmSample.getFilepath()))
                .map(rv -> Response.ok(rv).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Error getting rendering info for " + sampleId))
                        .build())
                ;
    }


    @ApiOperation(value = "Gets a calculated origin by Sample path",
            notes = "Returns a map of useful constants when creating a sample (origin, scaling)"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the sample path constants",  response = Map.class),
            @ApiResponse( code = 500, message = "Error occurred while fetching the sample constants" )
    })
    @GET
    @Path("sample/constants")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTmSampleConstants(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                         @ApiParam @QueryParam("samplePath") final String samplePath) {
        logger.trace("getTmSampleConstants(subjectKey: {}, samplePath: {})", subjectKey, samplePath);

        // read and process transform.txt file in Sample path
        // this is intended to be a one-time process and data returned will be stored in TmSample upon creation
        java.nio.file.Path sampleTransformPath = Paths.get(samplePath, "transform.txt");
        try {
            Map<String, Object> constants = new HashMap<>();
            Map<String, Integer> origin = new HashMap<>();
            Map<String, Double> scaling = new HashMap<>();
            BufferedReader reader = Files.newBufferedReader(sampleTransformPath);
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
                    .entity(constants)
                    .build();
        } catch (Exception e) {
            logger.error("Error reading transform constants for {} from {}", subjectKey, samplePath, e);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Error reading " + sampleTransformPath))
                    .build();
        }
    }

    @ApiOperation(value = "Creates a new TmSample",
            notes = "Creates a TmSample using the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully created a TmSample", response = TmSample.class),
            @ApiResponse( code = 500, message = "Error occurred while creating a TmSample" )
    })
    @PUT
    @Path("sample")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public TmSample createTmSample(DomainQuery query) {
        logger.trace("createTmSample({})", query);
        return tmSampleDao.createTmSample(query.getSubjectKey(), query.getDomainObjectAs(TmSample.class));
    }

    @ApiOperation(value = "Updates an existing TmSample",
            notes = "Updates a TmSample using the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully updated a TmSample", response = TmSample.class),
            @ApiResponse( code = 500, message = "Error occurred while updating a TmSample" )
    })
    @POST
    @Path("sample")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public TmSample updateTmSample(@ApiParam DomainQuery query) {
        logger.debug("updateTmSample({})", query);
        return tmSampleDao.updateTmSample(query.getSubjectKey(), query.getDomainObjectAs(TmSample.class));
    }

    @ApiOperation(value = "Removes a TmSample",
            notes = "Removes the TmSample using the TmSample Id"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully removed a TmSample"),
            @ApiResponse( code = 500, message = "Error occurred while removing a TmSample" )
    })
    @DELETE
    @Path("sample")
    public void removeTmSample(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                               @ApiParam @QueryParam("sampleId") final Long sampleId) {
        logger.debug("removeTmSample(subjectKey: {}, sampleId: {})", subjectKey, sampleId);
        tmSampleDao.removeTmSample(subjectKey, sampleId);
    }

}

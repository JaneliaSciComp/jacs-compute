package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
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
import org.janelia.jacs2.asyncservice.lvtservices.HortaDataManager;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.jacsstorage.clients.api.JadeStorageAttributes;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.domain.dao.TmSampleDao;
import org.janelia.model.domain.DomainConstants;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.tiledMicroscope.TmSample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Web service for CRUD operations having to do with Tiled Microscope domain objects.
 */
@Tag(name = "TmSample", description= "Janelia Mouselight Data Service")
@RequireAuthentication
@Produces("application/json")
@Path("/mouselight/data")
public class TmSampleResource {
    private static final Logger LOG = LoggerFactory.getLogger(TmSampleResource.class);

    @AsyncIndex
    @Inject
    private TmSampleDao tmSampleDao;
    @Inject
    private HortaDataManager hortaDataManager;

    @Operation(summary = "Gets a list of TM Samples",
            description = "Returns a list of all the TM Samples that are accessible by the current user"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully fetched the list of samples"),
            @ApiResponse(responseCode = "500", description = "Error occurred while fetching the samples")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("sample")
    public List<TmSample> getTmSamples(@QueryParam("subjectKey") final String subjectKey) {
        String sampleOwnerKey = StringUtils.defaultIfBlank(subjectKey, DomainConstants.MOUSELIGHT_GROUP_KEY);
        return tmSampleDao.findOwnedEntitiesBySubjectKey(sampleOwnerKey, 0, -1);
    }

    @Operation(
            summary = "Gets a TM Sample by id",
            description = "Returns the TM Sample identified by the given id"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully fetched the sample"),
            @ApiResponse(responseCode = "500", description = "Error occurred while fetching the sample")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("sample/{sampleId}")
    public TmSample getTmSample(@QueryParam("subjectKey") final String subjectKey,
                                @PathParam("sampleId") final Long sampleId) {
        return tmSampleDao.findEntityByIdReadableBySubjectKey(sampleId, subjectKey);
   }

    @Operation(summary = "Gets a calculated origin by Sample path",
            description = "Returns a map of useful constants when creating a sample (origin, scaling)"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully fetched the sample path constants"),
            @ApiResponse(responseCode = "500", description = "Error occurred while fetching the sample constants")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("sample/constants")
    public Response getTmSampleConstants(@Parameter @QueryParam("subjectKey") final String subjectKey,
                                         @Parameter @QueryParam("samplePath") final String samplePath,
                                         @Parameter @HeaderParam("AccessKey") String accessKey,
                                         @Parameter @HeaderParam("SecretKey") String secretKey,
                                         @Parameter @HeaderParam("AWSRegion") String awsRegion,
                                         @Context ContainerRequestContext containerRequestContext) {
        LOG.trace("getTmSampleConstants(subjectKey: {}, samplePath: {})", subjectKey, samplePath);
        if (StringUtils.isBlank(samplePath)) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("Invalid sample sample path - the sample path cannot be empty"))
                    .build();
        }
        String authSubjectKey = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
        JadeStorageAttributes storageAttributes = new JadeStorageAttributes()
                .setAttributeValue("AccessKey", accessKey)
                .setAttributeValue("SecretKey", secretKey)
                .setAttributeValue("AWSRegion", awsRegion);
        return hortaDataManager.getSampleConstants(authSubjectKey, samplePath, storageAttributes)
                .map(constants -> Response.ok()
                        .entity(new GenericEntity<Map<String, Object>>(constants){})
                        .build())
                .orElseGet(() -> {
                    LOG.error("Error reading transform constants for {} from {}", subjectKey, samplePath);
                    return Response.status(Response.Status.NOT_FOUND)
                            .entity(new ErrorResponse("Error reading transform.txt from " + samplePath))
                            .build();
                })
                ;
    }

    @Operation(summary = "Creates a new TmSample",
            description = "Creates a TmSample using the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Successfully created a TmSample"),
            @ApiResponse(responseCode = "500", description = "Error occurred while creating a TmSample")
    })
    @PUT
    @Path("sample")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createTmSample(@HeaderParam("AccessKey") String accessKey,
                                   @HeaderParam("SecretKey") String secretKey,
                                   @HeaderParam("AWSRegion") String awsRegion,
                                   DomainQuery query) {
        LOG.trace("createTmSample({})", query);
        TmSample sample = query.getDomainObjectAs(TmSample.class);
        if (StringUtils.isNotBlank(accessKey))
            sample.setStorageAttribute("AccessKey", accessKey);
        if (StringUtils.isNotBlank(secretKey))
            sample.setStorageAttribute("SecretKey", secretKey);
        if (StringUtils.isNotBlank(awsRegion))
            sample.setStorageAttribute("AWSRegion", awsRegion);
        String samplePath = sample.getLargeVolumeOctreeFilepath();
        LOG.info("Creating new TmSample {} with path {}", sample.getName(), samplePath);
        try {
            TmSample savedSample = hortaDataManager.createTmSample(query.getSubjectKey(), sample);
            LOG.info("Saved new sample as {}", savedSample);
            return Response.created(UriBuilder.fromMethod(this.getClass(), "getTmSample").build(savedSample.getId()))
                    .entity(savedSample)
                    .build();
        }
        catch (IOException e) {
            LOG.error("Error saving TmSample for path "+samplePath, e);
            // if transform.txt cannot be located at the sample path then don't even create the sample
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse(e.getMessage()))
                    .build();
        }
    }

    @Operation(summary = "Updates an existing TmSample",
            description = "Updates a TmSample using the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated a TmSample"),
            @ApiResponse(responseCode = "500", description = "Error occurred while updating a TmSample")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("sample")
    public TmSample updateTmSample(DomainQuery query) {
        LOG.trace("updateTmSample({})", query);
        String subjectKey = query.getSubjectKey();
        TmSample tmSample = query.getDomainObjectAs(TmSample.class);
        return hortaDataManager.updateSample(subjectKey, tmSample);
    }

    @Operation(summary = "Removes a TmSample",
            description = "Removes the TmSample using the TmSample Id"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully removed a TmSample"),
            @ApiResponse(responseCode = "500", description = "Error occurred while removing a TmSample")
    })
    @DELETE
    @Path("sample")
    public void removeTmSample(@QueryParam("subjectKey") final String subjectKey,
                               @QueryParam("sampleId") final Long sampleId) {
        LOG.trace("removeTmSample(subjectKey: {}, sampleId: {})", subjectKey, sampleId);
        tmSampleDao.removeTmSample(subjectKey, sampleId);
    }
}

package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
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
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.GenericEntity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.lvtservices.HortaDataManager;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.TmNeuronMetadataDao;
import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.tiledMicroscope.BoundingBox3d;
import org.janelia.model.domain.tiledMicroscope.BulkNeuronStyleUpdate;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmOperation;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "TmWorkspace", description = "Janelia Mouselight Data Service")
@Produces("application/json")
@Path("/mouselight/data")
public class TmWorkspaceResource {
    private static final Logger LOG = LoggerFactory.getLogger(TmWorkspaceResource.class);

    @Inject
    private LegacyDomainDao legacyWorkspaceDao;
    @AsyncIndex
    @Inject
    private TmWorkspaceDao tmWorkspaceDao;
    @Inject
    private TmNeuronMetadataDao tmNeuronMetadataDao;
    @Inject
    private HortaDataManager hortaDataManager;

    @Operation(summary = "Gets a list of TM Workspaces",
            description = "Returns a list of all the TM Workspaces that are accessible by the current user"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully fetched the list of workspaces"),
            @ApiResponse(responseCode = "500", description = "Error occurred while fetching the workspaces")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace")
    public List<TmWorkspace> getTmWorkspaces(@Parameter @QueryParam("subjectKey") String subjectKey,
                                             @Parameter @QueryParam("sampleId") Long sampleId,
                                             @Parameter @QueryParam("offset") Long offsetParam,
                                             @Parameter @QueryParam("length") Integer lengthParam) {
        LOG.info("getTmWorkspaces({}, sampleId={}, offset={}, length={})", subjectKey, sampleId, offsetParam, lengthParam);
        if (sampleId == null) {
            long offset = offsetParam != null ? offsetParam : 0;
            int length = lengthParam != null ? lengthParam : -1;
            return tmWorkspaceDao.findOwnedEntitiesBySubjectKey(subjectKey, offset, length);
        } else {
            return tmWorkspaceDao.getTmWorkspacesForSample(subjectKey, sampleId);
        }
    }

    @Operation(summary = "Gets a TM Workspace by id",
            description = "Returns the TM Workspace identified by the given id")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully fetched the workspace"),
            @ApiResponse(responseCode = "500", description = "Error occurred while fetching the workspace")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/{workspaceId}")
    public TmWorkspace getTmWorkspace(@Parameter @QueryParam("subjectKey") String subjectKey,
                                      @Parameter @PathParam("workspaceId") Long workspaceId) {
        LOG.info("getTmWorkspace({}, workspaceId={})", subjectKey, workspaceId);
        return tmWorkspaceDao.findEntityByIdReadableBySubjectKey(workspaceId, subjectKey);
    }

    @Operation(summary = "Creates a new TmWorkspace",
            description = "Creates a TmWorkspace using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully created a TmWorkspace"),
            @ApiResponse(responseCode = "500", description = "Error occurred while creating a TmWorkspace")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace")
    public TmWorkspace createTmWorkspace(DomainQuery query) {
        LOG.info("createTmWorkspace({})", query);
        return tmWorkspaceDao.createTmWorkspace(query.getSubjectKey(), query.getDomainObjectAs(TmWorkspace.class));
    }

    @Operation(summary = "Creates a copy of an existing TmWorkspace",
            description = "Creates a copy of the given TmWorkspace with a new name given by the parameter value of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully copies a TmWorkspace"),
            @ApiResponse(responseCode = "500", description = "Error occurred while copying a TmWorkspace")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/copy")
    public TmWorkspace copyTmWorkspace(@Parameter DomainQuery query) {
        LOG.info("copyTmWorkspace({})", query);
        return tmWorkspaceDao.copyTmWorkspace(query.getSubjectKey(), query.getDomainObjectAs(TmWorkspace.class), query.getPropertyValue(), query.getObjectType());
    }

    @Operation(summary = "Updates an existing TmWorkspace",
            description = "Updates a TmWorkspace using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated a TmWorkspace"),
            @ApiResponse(responseCode = "500", description = "Error occurred while updating a TmWorkspace")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace")
    public TmWorkspace updateTmWorkspace(@Parameter DomainQuery query) {
        LOG.info("updateTmWorkspace({})", query);
        return tmWorkspaceDao.updateTmWorkspace(query.getSubjectKey(), query.getDomainObjectAs(TmWorkspace.class));
    }

    @Operation(summary = "Removes a TmWorkspace",
            description = "Removes the TmWorkspace using the TmWorkspace Id")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully removed a TmWorkspace"),
            @ApiResponse(responseCode = "500", description = "Error occurred while removing a TmWorkspace")
    })
    @DELETE
    @Path("/workspace")
    public void removeTmWorkspace(@Parameter @QueryParam("subjectKey") final String subjectKey,
                                  @Parameter @QueryParam("workspaceId") final Long workspaceId) {
        LOG.info("removeTmWorkspace({}, workspaceId={})", subjectKey, workspaceId);
        TmWorkspace workspace = tmWorkspaceDao.findEntityByIdReadableBySubjectKey(workspaceId, subjectKey);
        hortaDataManager.removeWorkspace(subjectKey, workspace);
    }

    @Operation(summary = "Gets the neurons for a workspace",
            description = "Returns a list of neurons contained in a given workspace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully fetched neurons"),
            @ApiResponse(responseCode = "500", description = "Error occurred while occurred while fetching the neurons")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/workspace/neuron")
    public Response getWorkspaceNeurons(@Parameter @QueryParam("subjectKey") final String subjectKey,
                                        @Parameter @QueryParam("workspaceId") final Long workspaceId,
                                        @Parameter @QueryParam("offset") final Long offsetParam,
                                        @Parameter @QueryParam("length") final Integer lengthParam,
                                        @Parameter @QueryParam("frags") final Boolean fragsParam) {
        long offset = offsetParam == null || offsetParam < 0L ? 0 : offsetParam;
        boolean filterFrags = fragsParam == null ? false : fragsParam;
        LOG.info("getWorkspaceNeurons({}, workspaceId={}, offset={}, length={}, frags={})",
                subjectKey, workspaceId, offset, lengthParam, fragsParam);
        TmWorkspace workspace = tmWorkspaceDao.findEntityByIdReadableBySubjectKey(workspaceId, subjectKey);
        if (workspace==null)
            return null;
        int length = lengthParam == null || lengthParam < 0 ? -1 : lengthParam;
        try {
            Iterator<TmNeuronMetadata> foo = tmNeuronMetadataDao.streamWorkspaceNeurons(workspace,
                    subjectKey, offset, length, filterFrags).iterator();
            ObjectMapper mapper = new ObjectMapper();

            StreamingOutput stream = new StreamingOutput() {
                @Override
                public void write(OutputStream os) throws IOException, WebApplicationException {
                    Writer writer = new BufferedWriter(new OutputStreamWriter(os));
                    while (foo.hasNext()) {
                        TmNeuronMetadata nextNeuron = foo.next();
                        if (nextNeuron.isLargeNeuron()) {
                            // rehyrdate the large neuron
                            nextNeuron = tmNeuronMetadataDao.getTmNeuronMetadata(subjectKey, workspace, nextNeuron.getId());
                        }
                        mapper.writeValue(os, nextNeuron);
                    }
                    writer.flush();
                }
            };
            return Response.ok(stream).build();
        } catch (Exception e) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Error retrieving raw tile file info for sample "))
                    .build();
        }
    }

    @Operation(summary = "loads fragments into a workspace",
            description = "saves a list of 3D Bounding Boxes into a given workspace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully saved bounding boxes"),
            @ApiResponse(responseCode = "500", description = "Error occurred while saving the bounding boxes")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/workspace/boundingboxes")
    public Response saveWorkspaceBoundingBoxes(@Parameter @QueryParam("subjectKey") final String subjectKey,
                                              @Parameter @QueryParam("workspaceId") final Long workspaceId,
                                              List<BoundingBox3d> boxes) {
        LOG.info("saveWorkspaceBoundingBoxes({}, {})", subjectKey, workspaceId);
        TmWorkspace workspace = tmWorkspaceDao.findEntityByIdReadableBySubjectKey(workspaceId, subjectKey);
        if (workspace==null)
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Unable to find workspace"))
                    .build();
        try {
            tmWorkspaceDao.saveWorkspaceBoundingBoxes(workspace, boxes);
            workspace.setContainsFragments(true);
            tmWorkspaceDao.updateTmWorkspace(subjectKey, workspace);
            return Response.ok()
                    .build();
        } catch (Exception e) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Error saving bounding box info for workspace "))
                    .build();
        }
    }

    @Operation(summary = "Gets fragment bounding boxes for a workspace",
            description = "Returns a list of 3D Bounding Boxes contained in a given workspace")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully fetched bounding boxes"),
            @ApiResponse(responseCode = "500", description = "Error occurred while occurred while fetching the bounding boxes")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/boundingboxes")
    public Response getWorkspaceBoundingBoxes(@Parameter @QueryParam("subjectKey") final String subjectKey,
                                        @Parameter @QueryParam("workspaceId") final Long workspaceId) {
        LOG.info("getWorkspaceBoundingBoxes({}, {})", subjectKey, workspaceId);
        TmWorkspace workspace = tmWorkspaceDao.findEntityByIdReadableBySubjectKey(workspaceId, subjectKey);
        if (workspace==null)
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Unable to find workspace"))
                    .build();
        try {
            List<BoundingBox3d> boundingBoxList = tmWorkspaceDao.getWorkspaceBoundingBoxes(workspace.getId());
            return Response.ok()
                    .entity(new GenericEntity<List<BoundingBox3d>>(boundingBoxList){})
                    .build();
        } catch (Exception e) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Error retrieving raw tile file info for sample "))
                    .build();
        }
    }

    @Operation(summary = "Gets neuron metadata given a neuronId",
            description = "Returns a list of neurons given their ids")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully fetched neuron metadata"),
            @ApiResponse(responseCode = "500", description = "Error occurred while occurred while fetching the neurons")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/neuron/metadata")
    public List<TmNeuronMetadata> getWorkspaceNeurons(@Parameter @QueryParam("subjectKey") final String subjectKey,
                                                      @Parameter @QueryParam("workspaceId") final Long workspaceId,
                                                      List<Long> neuronIds) {
        LOG.info("getWorkspaceNeurons({}, workspaceId={}, neuronIds={})", subjectKey, workspaceId, DomainUtils.abbr(neuronIds));
        TmWorkspace workspace = tmWorkspaceDao.findEntityByIdReadableBySubjectKey(workspaceId, subjectKey);
        return tmNeuronMetadataDao.getTmNeuronMetadataByNeuronIds(workspace,
                neuronIds);
    }

    @Operation(summary = "Gets a count of the neurons in a workspace",
            description = "Returns a the number of neurons giving a workspace id")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully fetched neuron counts"),
            @ApiResponse(responseCode = "500", description = "Error occurred while occurred while processing the neuron count")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/neurons/totals")
    public Long getWorkspaceNeuronCount(@Parameter @QueryParam("subjectKey") final String subjectKey,
                                        @Parameter @QueryParam("workspaceId") final Long workspaceId) {
        LOG.info("getWorkspaceNeuronCount({}, workspaceId={})", subjectKey, workspaceId);
        TmWorkspace workspace = tmWorkspaceDao.findEntityByIdReadableBySubjectKey(workspaceId, subjectKey);
        return tmNeuronMetadataDao.getNeuronCountsForWorkspace(workspace, subjectKey);
    }

    @Operation(summary = "Bulk update neuron styles",
            description = "Update style for a list of neurons")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully bulk updated styles"),
            @ApiResponse(responseCode = "500", description = "Error occurred while bulk updating styles")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/neuronStyle")
    public Response updateNeuronStyles(@Parameter @QueryParam("subjectKey") final String subjectKey,
                                       @Parameter @QueryParam("workspaceId") final Long workspaceId,
                                       @Parameter final BulkNeuronStyleUpdate bulkNeuronStyleUpdate) {
        LOG.info("updateNeuronStyles({}, {})", subjectKey, bulkNeuronStyleUpdate);
        TmWorkspace workspace = tmWorkspaceDao.findEntityByIdReadableBySubjectKey(workspaceId, subjectKey);
        if (bulkNeuronStyleUpdate.getVisible() == null && !StringUtils.isNotBlank(bulkNeuronStyleUpdate.getColorHex())) {
            LOG.warn("Cannot have both visible and colorhex unset");
            return Response.status(Response.Status.BAD_REQUEST).build();
        } else {
            tmNeuronMetadataDao.updateNeuronStyles(bulkNeuronStyleUpdate, workspace, subjectKey);
            return Response.ok("DONE").build();
        }
    }

    @Operation(summary = "Add or remove tags",
            description = "Add or remove the given tags to a list of neurons")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully bulk updated tags"),
            @ApiResponse(responseCode = "500", description = "Error occurred while bulk updating tags")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/neuronTags")
    public Response addNeuronTags(@Parameter @QueryParam("subjectKey") final String subjectKey,
                                  @Parameter @QueryParam("tags") final String tags,
                                  @Parameter @QueryParam("tagState") final boolean tagState,
                                  @Parameter @QueryParam("workspaceId") final Long workspaceId,
                                  @Parameter final List<Long> neuronIds) {
        TmWorkspace workspace = tmWorkspaceDao.findEntityByIdReadableBySubjectKey(workspaceId, subjectKey);
        List<String> tagList = Arrays.asList(StringUtils.split(tags, ","));
        LOG.info("addNeuronTag({}, tags={}, tagState={}, workspaceId={}, neuronIds={})",
                subjectKey, DomainUtils.abbr(tagList), tagState, workspaceId, DomainUtils.abbr(neuronIds));
        if (neuronIds.isEmpty()) {
            LOG.warn("Neuron IDs cannot be empty");
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        if (tags.isEmpty()) {
            LOG.warn("Tag list cannot be empty");
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        tmNeuronMetadataDao.updateNeuronTagsForNeurons(workspace, neuronIds, tagList, tagState, subjectKey);
        return Response.ok("DONE").build();
    }


    @Operation(summary = "Creates an TM Operation log for an operation performed during neuron tracing",
            description = "Stores the operation log in the TmOperation table for future analysis")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully created an operation log"),
            @ApiResponse(responseCode = "500", description = "Error occurred while creating the operation log")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/operation/log")
    public void createOperationLog(@Parameter @QueryParam("username") String subjectKey,
                                   @Parameter @QueryParam("sampleId") Long sampleId,
                                   @Parameter @QueryParam("workspaceId") Long workspaceId,
                                   @Parameter @QueryParam("neuronId") Long neuronId,
                                   @Parameter @QueryParam("activity") TmOperation.Activity activity,
                                   @Parameter @QueryParam("elapsedTime") Long elapsedTime,
                                   @Parameter @QueryParam("timestamp") String timestamp) {
        try {
            DateFormat format = new SimpleDateFormat("MM-dd-yyyy hh:mm:ss");
            Date timestampDate = null;
            if (timestamp!=null) {
                timestampDate = format.parse(timestamp);
            }
            tmNeuronMetadataDao.createOperationLog(sampleId, workspaceId,neuronId, activity, timestampDate, elapsedTime, subjectKey);
        } catch (Exception e) {
            LOG.error("Error occurred creating operation log for {},{},{},{},{}", subjectKey,workspaceId,neuronId,
                    activity,timestamp);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @Operation(summary = "gets Operation logs based off username, workspace or timestamp range",
            description = "returns a list of operation logs based off the query")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved operation logs"),
            @ApiResponse(responseCode = "500", description = "Error occurred while getting the operation logs")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/operation/log/search")
    public  List<TmOperation> getOperationLog(@Parameter @QueryParam("username") String subjectKey,
                                                @Parameter @QueryParam("workspaceId") Long workspaceId,
                                                @Parameter @QueryParam("neuronId") Long neuronId,
                                                @Parameter @QueryParam("startTime") String startTime,
                                                @Parameter @QueryParam("endTime") String endTime) {
        try {
            DateFormat format = new SimpleDateFormat("MM-dd-yyyy hh:mm:ss");
            Date startTimeDate = null, endTimeDate = null;
            if (startTime!=null) {
                startTimeDate = format.parse(startTime);
            }
            if (endTime!=null) {
                endTimeDate = format.parse(endTime);
            }
            return tmNeuronMetadataDao.getOperations(workspaceId, neuronId, startTimeDate, endTimeDate);
        } catch (Exception e) {
            LOG.error("Error occurred getting operation log for {},{},{},{},{}", subjectKey,workspaceId,neuronId, startTime, endTime);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }}

package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

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
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import io.swagger.models.Operation;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.lvtservices.HortaDataManager;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.TmNeuronMetadataDao;
import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.tiledMicroscope.*;
import org.janelia.model.domain.workspace.Workspace;
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
        value = "Janelia Mouselight Data Service",
        authorizations = {
                @Authorization("user"),
                @Authorization("runAs")
        }
)

@ApplicationScoped
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

    // TODO: this doesn't seem to belong here, but I'm just commenting it until we can verify
    //       (with the 9.14 release) that nothing is actually calling this.
//    @ApiOperation(value = "Gets all the Workspaces a user can read",
//            notes = "Returns all the Workspaces which are visible to the current user."
//    )
//    @ApiResponses(value = {
//            @ApiResponse(
//                    code = 200, message = "Successfully got all workspaces",
//                    response = Workspace.class,
//                    responseContainer = "List"),
//            @ApiResponse(code = 500, message = "Internal Server Error getting workspaces")
//    })
//    @GET
//    @Produces(MediaType.APPLICATION_JSON)
//    @Path("/workspaces")
//    public List<Workspace> getAllWorkspaces(@QueryParam("subjectKey") String subjectKey) {
//        LOG.info("getAllWorkspace({})", subjectKey);
//        try {
//            return legacyWorkspaceDao.getWorkspaces(subjectKey);
//        } catch (Exception e) {
//            LOG.error("Error occurred getting default workspace for {}", subjectKey, e);
//            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
//        } finally {
//            LOG.trace("Finished getAllWorkspace({})", subjectKey);
//        }
//    }

    @ApiOperation(value = "Gets a list of TM Workspaces",
            notes = "Returns a list of all the TM Workspaces that are accessible by the current user"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched the list of workspaces", response = TmWorkspace.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Error occurred while fetching the workspaces")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace")
    public List<TmWorkspace> getTmWorkspaces(@ApiParam @QueryParam("subjectKey") String subjectKey,
                                             @ApiParam @QueryParam("sampleId") Long sampleId,
                                             @ApiParam @QueryParam("offset") Long offsetParam,
                                             @ApiParam @QueryParam("length") Integer lengthParam) {
        LOG.info("getTmWorkspaces({}, sampleId={}, offset={}, length={})", subjectKey, sampleId, offsetParam, lengthParam);
        if (sampleId == null) {
            long offset = offsetParam != null ? offsetParam : 0;
            int length = lengthParam != null ? lengthParam : -1;
            return tmWorkspaceDao.findOwnedEntitiesBySubjectKey(subjectKey, offset, length);
        } else {
            return tmWorkspaceDao.getTmWorkspacesForSample(subjectKey, sampleId);
        }
    }

    @ApiOperation(value = "Gets a TM Workspace by id",
            notes = "Returns the TM Workspace identified by the given id"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched the workspace", response = TmWorkspace.class),
            @ApiResponse(code = 500, message = "Error occurred while fetching the workspace")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/{workspaceId}")
    public TmWorkspace getTmWorkspace(@ApiParam @QueryParam("subjectKey") String subjectKey,
                                      @ApiParam @PathParam("workspaceId") Long workspaceId) {
        LOG.info("getTmWorkspace({}, workspaceId={})", subjectKey, workspaceId);
        return tmWorkspaceDao.findEntityByIdReadableBySubjectKey(workspaceId, subjectKey);
    }

    @ApiOperation(value = "Creates a new TmWorkspace",
            notes = "Creates a TmWorkspace using the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully created a TmWorkspace", response = TmWorkspace.class),
            @ApiResponse(code = 500, message = "Error occurred while creating a TmWorkspace")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace")
    public TmWorkspace createTmWorkspace(DomainQuery query) {
        LOG.info("createTmWorkspace({})", query);
        return tmWorkspaceDao.createTmWorkspace(query.getSubjectKey(), query.getDomainObjectAs(TmWorkspace.class));
    }

    @ApiOperation(value = "Creates a copy of an existing TmWorkspace",
            notes = "Creates a copy of the given TmWorkspace with a new name given by the parameter value of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully copies a TmWorkspace", response = TmWorkspace.class),
            @ApiResponse(code = 500, message = "Error occurred while copying a TmWorkspace")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/copy")
    public TmWorkspace copyTmWorkspace(@ApiParam DomainQuery query) {
        LOG.info("copyTmWorkspace({})", query);
        return tmWorkspaceDao.copyTmWorkspace(query.getSubjectKey(), query.getDomainObjectAs(TmWorkspace.class), query.getPropertyValue(), query.getObjectType());
    }

    @ApiOperation(value = "Updates an existing TmWorkspace",
            notes = "Updates a TmWorkspace using the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated a TmWorkspace", response = TmWorkspace.class),
            @ApiResponse(code = 500, message = "Error occurred while updating a TmWorkspace")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace")
    public TmWorkspace updateTmWorkspace(@ApiParam DomainQuery query) {
        LOG.info("updateTmWorkspace({})", query);
        return tmWorkspaceDao.updateTmWorkspace(query.getSubjectKey(), query.getDomainObjectAs(TmWorkspace.class));
    }

    @ApiOperation(value = "Removes a TmWorkspace",
            notes = "Removes the TmWorkspace using the TmWorkspace Id"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully removed a TmWorkspace"),
            @ApiResponse(code = 500, message = "Error occurred while removing a TmWorkspace")
    })
    @DELETE
    @Path("/workspace")
    public void removeTmWorkspace(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                  @ApiParam @QueryParam("workspaceId") final Long workspaceId) {
        LOG.info("removeTmWorkspace({}, workspaceId={})", subjectKey, workspaceId);
        TmWorkspace workspace = tmWorkspaceDao.findEntityByIdReadableBySubjectKey(workspaceId, subjectKey);
        hortaDataManager.removeWorkspace(subjectKey, workspace);
    }

    @ApiOperation(value = "Gets the neurons for a workspace",
            notes = "Returns a list of neurons contained in a given workspace"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched neurons", response = List.class),
            @ApiResponse(code = 500, message = "Error occurred while occurred while fetching the neurons")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @Path("/workspace/neuron")
    public Response getWorkspaceNeurons(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                        @ApiParam @QueryParam("workspaceId") final Long workspaceId,
                                        @ApiParam @QueryParam("offset") final Long offsetParam,
                                        @ApiParam @QueryParam("length") final Integer lengthParam,
                                        @ApiParam @QueryParam("frags") final Boolean fragsParam) {
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

    @ApiOperation(value = "loads fragments into a workspace",
            notes = "saves a list of 3D Bounding Boxes into a given workspace"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully saved bounding boxes", response = List.class),
            @ApiResponse(code = 500, message = "Error occurred while saving the bounding boxes")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/workspace/boundingboxes")
    public Response saveWorkspaceBoundingBoxes(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                              @ApiParam @QueryParam("workspaceId") final Long workspaceId,
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

    @ApiOperation(value = "Gets fragment bounding boxes for a workspace",
            notes = "Returns a list of 3D Bounding Boxes contained in a given workspace"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched bounding boxes", response = List.class),
            @ApiResponse(code = 500, message = "Error occurred while occurred while fetching the bounding boxes")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/boundingboxes")
    public Response getWorkspaceBoundingBoxes(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                        @ApiParam @QueryParam("workspaceId") final Long workspaceId) {
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

    @ApiOperation(value = "Gets neuron metadata given a neuronId",
            notes = "Returns a list of neurons given their ids"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched neuron metadata", response = List.class),
            @ApiResponse(code = 500, message = "Error occurred while occurred while fetching the neurons")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/neuron/metadata")
    public List<TmNeuronMetadata> getWorkspaceNeurons(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                                      @ApiParam @QueryParam("workspaceId") final Long workspaceId,
                                                      List<Long> neuronIds) {
        LOG.info("getWorkspaceNeurons({}, workspaceId={}, neuronIds={})", subjectKey, workspaceId, DomainUtils.abbr(neuronIds));
        TmWorkspace workspace = tmWorkspaceDao.findEntityByIdReadableBySubjectKey(workspaceId, subjectKey);
        return tmNeuronMetadataDao.getTmNeuronMetadataByNeuronIds(workspace,
                neuronIds);
    }

    @ApiOperation(value = "Gets a count of the neurons in a workspace",
            notes = "Returns a the number of neurons giving a workspace id"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched neuron counts", response = List.class),
            @ApiResponse(code = 500, message = "Error occurred while occurred while processing the neuron count")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/neurons/totals")
    public Long getWorkspaceNeuronCount(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                        @ApiParam @QueryParam("workspaceId") final Long workspaceId) {
        LOG.info("getWorkspaceNeuronCount({}, workspaceId={})", subjectKey, workspaceId);
        TmWorkspace workspace = tmWorkspaceDao.findEntityByIdReadableBySubjectKey(workspaceId, subjectKey);
        return tmNeuronMetadataDao.getNeuronCountsForWorkspace(workspace, subjectKey);
    }

    @ApiOperation(value = "Bulk update neuron styles",
            notes = "Update style for a list of neurons"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully bulk updated styles"),
            @ApiResponse(code = 500, message = "Error occurred while bulk updating styles")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/neuronStyle")
    public Response updateNeuronStyles(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                       @ApiParam @QueryParam("workspaceId") final Long workspaceId,
                                       @ApiParam final BulkNeuronStyleUpdate bulkNeuronStyleUpdate) {
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

    @ApiOperation(value = "Add or remove tags",
            notes = "Add or remove the given tags to a list of neurons"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully bulk updated tags"),
            @ApiResponse(code = 500, message = "Error occurred while bulk updating tags")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/neuronTags")
    public Response addNeuronTags(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                  @ApiParam @QueryParam("tags") final String tags,
                                  @ApiParam @QueryParam("tagState") final boolean tagState,
                                  @ApiParam @QueryParam("workspaceId") final Long workspaceId,
                                  @ApiParam final List<Long> neuronIds) {
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

    @ApiOperation(value = "Retrieves the largest TmWorkspaces in terms of size including the space they take up " +
            "in the Mongo Database",
            notes = ""
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully created an operation log"),
            @ApiResponse(code = 500, message = "Error occurred while creating the operation log")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/largest")
    public List<TmWorkspaceInfo> getLargestWorkspaces(@ApiParam @QueryParam("username") String subjectKey,
                                                      @ApiParam @QueryParam("limit") Long limitParam) {
        try {
            long limit = limitParam == null ? 20L : limitParam;
            return tmWorkspaceDao.getLargestWorkspaces(subjectKey, limit);
        } catch (Exception e) {
            LOG.error("Error occurred trying to retrieve largest workspaces report", e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }


    @ApiOperation(value = "Removes a list of workspaces, including their neurons and bounding boxes",
            notes = ""
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully removed TmWorkspaces", response = List.class),
            @ApiResponse(code = 500, message = "Error occurred while removing the workspaces")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/workspaces/remove")
    public Response removeWorkspaces(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                               List<Long> workspaceIds) {
        LOG.info("removing TmWorkspaces ({}, {})", subjectKey, workspaceIds);
        for (Long workspaceId : workspaceIds) {
           tmWorkspaceDao.deleteByIdAndSubjectKey(workspaceId, subjectKey);
        }
        return Response.ok()
                .build();
    }


    @ApiOperation(value = "Creates an TM Operation log for an operation performed during neuron tracing",
            notes = "Stores the operation log in the TmOperation table for future analysis"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully created an operation log"),
            @ApiResponse(code = 500, message = "Error occurred while creating the operation log")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/operation/log")
    public void createOperationLog(@ApiParam @QueryParam("username") String subjectKey,
                                   @ApiParam @QueryParam("operation") TmOperation operation) {
        try {
            tmNeuronMetadataDao.createOperationLog(operation, subjectKey);
        } catch (Exception e) {
            LOG.error("Error occurred creating operation log for {},{},{},{},{}", subjectKey, operation.getWorkspaceId(),
                    operation.getNeuronId(), operation.getTimestamp(), operation.getElapsedTime());
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "gets Operation logs based off username, workspace or timestamp range",
            notes = "returns a list of operation logs based off the query"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved operation logs"),
            @ApiResponse(code = 500, message = "Error occurred while getting the operation logs")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/operation/log/search")
    public  List<TmOperation> getOperationLog(@ApiParam @QueryParam("username") String subjectKey,
                                                @ApiParam @QueryParam("workspaceId") Long workspaceId,
                                                @ApiParam @QueryParam("neuronId") Long neuronId,
                                                @ApiParam @QueryParam("startTime") String startTime,
                                                @ApiParam @QueryParam("endTime") String endTime) {
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

package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.TmNeuronMetadataDao;
import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.tiledMicroscope.BulkNeuronStyleUpdate;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
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
@RequireAuthentication
@ApplicationScoped
@Produces("application/json")
@Path("/mouselight/data")
public class TmResource {

    private static final Logger LOG = LoggerFactory.getLogger(TmResource.class);

    @Inject
    private LegacyDomainDao legacyWorkspaceDao;
    @AsyncIndex
    @Inject
    private TmWorkspaceDao tmWorkspaceDao;
    @AsyncIndex
    @Inject
    private TmNeuronMetadataDao tmNeuronMetadataDao;

    @ApiOperation(value = "Gets all the Workspaces a user can read",
            notes = "Returns all the Workspaces which are visible to the current user."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = 200, message = "Successfully got all workspaces",
                    response = Workspace.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting workspaces")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspaces")
    public List<Workspace> getAllWorkspaces(@QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getAllWorkspace({})", subjectKey);
        try {
            return legacyWorkspaceDao.getWorkspaces(subjectKey);
        } catch (Exception e) {
            LOG.error("Error occurred getting default workspace for {}", subjectKey, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished getAllWorkspace({})", subjectKey);
        }
    }

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
        LOG.trace("getTmWorkspaces({}, sampleId={})", subjectKey, sampleId);
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
        LOG.debug("getTmWorkspace({}, workspaceId={})", subjectKey, workspaceId);
        return tmWorkspaceDao.findEntityByIdAccessibleBySubjectKey(workspaceId, subjectKey);
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
        LOG.trace("createTmWorkspace({})", query);
        TmWorkspace tmWorkspace = tmWorkspaceDao.createTmWorkspace(query.getSubjectKey(), query.getDomainObjectAs(TmWorkspace.class));
        return tmWorkspace;
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
        LOG.debug("copyTmWorkspace({})", query);
        TmWorkspace tmWorkspace = tmWorkspaceDao.copyTmWorkspace(query.getSubjectKey(), query.getDomainObjectAs(TmWorkspace.class), query.getPropertyValue(), query.getObjectType());
        return tmWorkspace;
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
        LOG.debug("updateTmWorkspace({})", query);
        TmWorkspace tmWorkspace = tmWorkspaceDao.updateTmWorkspace(query.getSubjectKey(), query.getDomainObjectAs(TmWorkspace.class));
        return tmWorkspace;
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
        LOG.debug("removeTmWorkspace({}, workspaceId={})", subjectKey, workspaceId);
        tmWorkspaceDao.deleteByIdAndSubjectKey(workspaceId, subjectKey);
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
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/neuron")
    public List<TmNeuronMetadata> getWorkspaceNeurons(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                                             @ApiParam @QueryParam("workspaceId") final Long workspaceId,
                                                             @ApiParam @QueryParam("offset") final Long offsetParam,
                                                             @ApiParam @QueryParam("length") final Integer lengthParam) {
        LOG.info("getWorkspaceNeuronMetadata({}, {}, {})", workspaceId, offsetParam, lengthParam);
        long offset = offsetParam == null || offsetParam < 0L ? 0 : offsetParam;
        int length = lengthParam == null || lengthParam < 0 ? -1 : lengthParam;
        return tmNeuronMetadataDao.getTmNeuronMetadataByWorkspaceId(subjectKey, workspaceId, offset, length);
    }

    @ApiOperation(value = "Creates a new neuron",
            notes = "Creates a neuron in the given workspace"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully created a TmNeuron", response = TmNeuronMetadata.class),
            @ApiResponse(code = 500, message = "Error occurred while creating a TmNeuron")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/neuron")
    public TmNeuronMetadata createTmNeuron(DomainQuery query) {
        TmNeuronMetadata neuron = query.getDomainObjectAs(TmNeuronMetadata.class);
        String subjectKey = query.getSubjectKey();
        LOG.info("createTmNeuron({}, {})", subjectKey, neuron);
        TmWorkspace workspace = tmWorkspaceDao.findEntityByIdAccessibleBySubjectKey(neuron.getWorkspaceId(), subjectKey);
        if (workspace == null) {
            LOG.info("No workspace found for {} accessible by {}", neuron.getWorkspaceId(), subjectKey);
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        } else {
            TmNeuronMetadata newNeuron = tmNeuronMetadataDao.createTmNeuronInWorkspace(subjectKey, neuron, workspace);
            return newNeuron;
        }
    }

    @ApiOperation(value = "Updates existing neurons",
            notes = "Updates a list of neurons' metadata and ProtoBuf-serialized annotations"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated TmNeurons", response = List.class),
            @ApiResponse(code = 500, message = "Error occurred while updating TmNeurons")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/neuron")
    public TmNeuronMetadata updateTmNeuron(DomainQuery query) {
        TmNeuronMetadata neuron = query.getDomainObjectAs(TmNeuronMetadata.class);
        String subjectKey = query.getSubjectKey();

        LOG.info("updateTmNeurons({}, numNeurons={})", subjectKey, neuron);

        TmNeuronMetadata updatedNeuron = tmNeuronMetadataDao.saveBySubjectKey(neuron, subjectKey);

        /* List<TmNeuronMetadata> list = new ArrayList<>();
        if (neurons.size()==0)
            return null;
        for (int i = 0; i < neurons.size(); i += 2) {
            TmNeuronMetadata neuron = neurons.get(i);
            TmNeuronMetadata updatedNeuron = tmNeuronMetadataDao.saveBySubjectKey(neuron, subjectKey);
            list.add(updatedNeuron);
        }
        if (list.size() > 1) {
            LOG.trace("{} updated {} neurons in workspace {}",
                    subjectKey, list.size(), list.stream().map(TmNeuronMetadata::getWorkspaceId).collect(Collectors.toSet()));
        } else if (list.size() == 1) {
            LOG.trace("{} updated neuron {} in workspace {}",
                    subjectKey, list.get(0).getId(), list.get(0).getWorkspaceId());
        }*/
        return updatedNeuron;
    }

    @ApiOperation(value = "Gets neuron metadata given a neuronId",
            notes = "Returns a list of neurons given their ids"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched neuron metadata", response = List.class),
            @ApiResponse(code = 500, message = "Error occurred while occurred while fetching the neurons")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/neuron/metadata")
    public List<TmNeuronMetadata> getWorkspaceNeurons(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                                      @ApiParam @QueryParam("neuronIds") final List<Long> neuronIds) {
        LOG.info("getNeuronMetadata({}, neuronIds={})", subjectKey, neuronIds);
        return tmNeuronMetadataDao.findEntitiesByIdsAccessibleBySubjectKey(neuronIds, subjectKey);
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
                                       @ApiParam final BulkNeuronStyleUpdate bulkNeuronStyleUpdate) {
        LOG.debug("updateNeuronStyles({}, {})", subjectKey, bulkNeuronStyleUpdate);
        if (bulkNeuronStyleUpdate.getVisible() == null && StringUtils.isNotBlank(bulkNeuronStyleUpdate.getColorHex())) {
            LOG.warn("Cannot have both visible and colorhex unset");
            return Response.status(Response.Status.BAD_REQUEST).build();
        } else {
            tmNeuronMetadataDao.updateNeuronStyles(bulkNeuronStyleUpdate, subjectKey);
            return Response.ok("DONE").build();
        }
    }

    @ApiOperation(value = "Removes an existing neuron",
            notes = "Removes the neuron by its id"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully removed a TmNeuron"),
            @ApiResponse(code = 500, message = "Error occurred while removing a TmNeuron")
    })
    @DELETE
    @Path("/workspace/neuron")
    public void removeTmNeuron(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                               @ApiParam @QueryParam("neuronId") final Long neuronId) {
        LOG.debug("removeTmNeuron({}, neuronId={})", subjectKey, neuronId);
        tmNeuronMetadataDao.removeTmNeuron(neuronId, subjectKey);
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
                                  @ApiParam final List<Long> neuronIds) {
        List<String> tagList = Arrays.asList(StringUtils.split(tags, ","));
        LOG.debug("addNeuronTag({}, neuronIds={}, tag={}, tagState={})",
                subjectKey, DomainUtils.abbr(neuronIds), DomainUtils.abbr(tagList), tagState);
        if (neuronIds.isEmpty()) {
            LOG.warn("Neuron IDs cannot be empty");
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        if (tags.isEmpty()) {
            LOG.warn("Tag list cannot be empty");
            return Response.status(Response.Status.BAD_REQUEST).build();
        }
        tmNeuronMetadataDao.updateNeuronTagsTagsForNeurons(neuronIds, tagList, tagState, subjectKey);
        return Response.ok("DONE").build();
    }

}

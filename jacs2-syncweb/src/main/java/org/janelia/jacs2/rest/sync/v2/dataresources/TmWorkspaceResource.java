package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.tuple.Pair;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.BodyPartEntity;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartMediaTypes;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.TmNeuronMetadataDao;
import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
import org.janelia.model.domain.workspace.Workspace;
import org.slf4j.Logger;

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
import java.util.List;
import java.util.stream.Collectors;

@Api(value = "Workspace Data Service")
@RequireAuthentication
@ApplicationScoped
@Produces("application/json")
@Path("/mouselight/data")
public class TmWorkspaceResource {

    @Inject private LegacyDomainDao legacyWorkspaceDao;
    @Inject private TmWorkspaceDao tmWorkspaceDao;
    @Inject private TmNeuronMetadataDao tmNeuronMetadataDao;
    @Inject private Logger logger;

    @ApiOperation(value = "Gets all the Workspaces a user can read",
            notes = "Returns all the Workspaces which are visible to the current user."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = 200, message = "Successfully got all workspaces",
                    response = Workspace.class,
                    responseContainer =  "List"),
            @ApiResponse( code = 500, message = "Internal Server Error getting workspaces" )
    })
    @GET
    @Path("/workspaces")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Workspace> getAllWorkspaces(@QueryParam("subjectKey") String subjectKey) {
        logger.trace("Start getAllWorkspace({})",subjectKey);
        try {
            return legacyWorkspaceDao.getWorkspaces(subjectKey);
        } catch (Exception e) {
            logger.error("Error occurred getting default workspace for {}", subjectKey, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            logger.trace("Finished getAllWorkspace({})", subjectKey);
        }
    }

    @ApiOperation(value = "Gets a list of TM Workspaces",
            notes = "Returns a list of all the TM Workspaces that are accessible by the current user"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the list of workspaces",  response = TmWorkspace.class,
                    responseContainer = "List" ),
            @ApiResponse( code = 500, message = "Error occurred while fetching the workspaces" )
    })
    @GET
    @Path("/workspace")
    @Produces(MediaType.APPLICATION_JSON)
    public List<TmWorkspace> getTmWorkspaces(@ApiParam @QueryParam("subjectKey") String subjectKey,
                                             @ApiParam @QueryParam("sampleId") Long sampleId) {
        logger.trace("getTmWorkspaces({}, sampleId={})", subjectKey, sampleId);
        if (sampleId == null) {
            return tmWorkspaceDao.findByOwnerKey(subjectKey);
        } else {
            return tmWorkspaceDao.getTmWorkspacesForSample(subjectKey, sampleId);
        }
    }

    @ApiOperation(value = "Gets a TM Workspace by id",
            notes = "Returns the TM Workspace identified by the given id"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the workspace",  response = TmWorkspace.class),
            @ApiResponse( code = 500, message = "Error occurred while fetching the workspace" )
    })
    @GET
    @Path("/workspace/{workspaceId}")
    @Produces(MediaType.APPLICATION_JSON)
    public TmWorkspace getTmWorkspace(@ApiParam @QueryParam("subjectKey") String subjectKey,
                                      @ApiParam @PathParam("workspaceId") Long workspaceId) {
        logger.debug("getTmWorkspace({}, workspaceId={})", subjectKey, workspaceId);
        return tmWorkspaceDao.findByIdAndSubjectKey(workspaceId, subjectKey);
    }

    @ApiOperation(value = "Creates a new TmWorkspace",
            notes = "Creates a TmWorkspace using the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully created a TmWorkspace", response = TmWorkspace.class),
            @ApiResponse( code = 500, message = "Error occurred while creating a TmWorkspace" )
    })
    @PUT
    @Path("/workspace")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public TmWorkspace createTmWorkspace(DomainQuery query) {
        logger.trace("createTmWorkspace({})", query);
        return tmWorkspaceDao.createTmWorkspace(query.getSubjectKey(), query.getDomainObjectAs(TmWorkspace.class));
    }

    @POST
    @Path("/workspace/copy")
    @ApiOperation(value = "Creates a copy of an existing TmWorkspace",
            notes = "Creates a copy of the given TmWorkspace with a new name given by the parameter value of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully copies a TmWorkspace", response = TmWorkspace.class),
            @ApiResponse( code = 500, message = "Error occurred while copying a TmWorkspace" )
    })
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public TmWorkspace copyTmWorkspace(@ApiParam DomainQuery query) {
        logger.debug("copyTmWorkspace({})", query);
        return tmWorkspaceDao.copyTmWorkspace(query.getSubjectKey(), query.getDomainObjectAs(TmWorkspace.class), query.getPropertyValue(), (String)query.getObjectType());
    }

    @ApiOperation(value = "Updates an existing TmWorkspace",
            notes = "Updates a TmWorkspace using the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully updated a TmWorkspace", response = TmWorkspace.class),
            @ApiResponse( code = 500, message = "Error occurred while updating a TmWorkspace" )
    })
    @POST
    @Path("/workspace")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public TmWorkspace updateTmWorkspace(@ApiParam DomainQuery query) {
        logger.debug("updateTmWorkspace({})", query);
        return tmWorkspaceDao.updateTmWorkspace(query.getSubjectKey(), query.getDomainObjectAs(TmWorkspace.class));
    }

    @ApiOperation(value = "Removes a TmWorkspace",
            notes = "Removes the TmWorkspace using the TmWorkspace Id"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully removed a TmWorkspace"),
            @ApiResponse( code = 500, message = "Error occurred while removing a TmWorkspace" )
    })
    @DELETE
    @Path("/workspace")
    public void removeTmWorkspace(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                  @ApiParam @QueryParam("workspaceId") final Long workspaceId) {
        logger.debug("removeTmWorkspace({}, workspaceId={})", subjectKey, workspaceId);
        tmWorkspaceDao.deleteByIdAndSubjectKey(workspaceId, subjectKey);
    }

    @ApiOperation(value = "Gets the neurons for a workspace",
            notes = "Returns a list of neurons contained in a given workspace"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched neurons", response = List.class),
            @ApiResponse( code = 500, message = "Error occurred while occurred while fetching the neurons" )
    })
    @GET
    @Path("/workspace/neuron/metadata")
    @Produces(MediaType.APPLICATION_JSON)
    public List<TmNeuronMetadata> getWorkspaceNeuronMetadata(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                                             @ApiParam @QueryParam("workspaceId") final Long workspaceId) {
        logger.info("getWorkspaceNeuronMetadata({})", workspaceId);
        return tmNeuronMetadataDao.getTmNeuronMetadataByWorkspaceId(subjectKey, workspaceId);
    }

    @GET
    @Path("/workspace/neuron")
    @ApiOperation(value = "Gets the neurons for a workspace",
            notes = "Returns a list of neurons contained in a given workspace"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched neurons"),
            @ApiResponse( code = 500, message = "Error occurred while occurred while fetching the neurons" )
    })
    @Produces(MultiPartMediaTypes.MULTIPART_MIXED)
    public Response getWorkspaceNeurons(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                        @ApiParam @QueryParam("workspaceId") final Long workspaceId) {
        logger.info("getWorkspaceNeurons({}, workspaceId={})", subjectKey, workspaceId);
        MultiPart multiPartEntity = new MultiPart();
        TmWorkspace workspace = tmWorkspaceDao.findByIdAndSubjectKey(workspaceId, subjectKey);
        if (workspace == null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Error getting the workspace " + workspaceId + " for " + subjectKey))
                    .build();
        }
        List<Pair<TmNeuronMetadata, InputStream>> neuronPairs = tmNeuronMetadataDao.getTmNeuronsMetadataWithPointStreamsByWorkspaceId(subjectKey, workspace);
        if (neuronPairs.isEmpty()) {
            multiPartEntity.bodyPart(new BodyPart("Empty", MediaType.TEXT_PLAIN_TYPE));
        } else {
            for (Pair<TmNeuronMetadata, InputStream> neuronPair : neuronPairs) {
                multiPartEntity.bodyPart(new BodyPart(neuronPair.getLeft(), MediaType.APPLICATION_JSON_TYPE));
                multiPartEntity.bodyPart(new BodyPart(neuronPair.getRight(), MediaType.APPLICATION_OCTET_STREAM_TYPE));
            }
        }
        return Response.ok().entity(multiPartEntity).type(MultiPartMediaTypes.MULTIPART_MIXED).build();
    }

    @ApiOperation(value = "Creates a new neuron",
            notes = "Creates a neuron in the given workspace"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully created a TmNeuron", response = TmNeuronMetadata.class),
            @ApiResponse( code = 500, message = "Error occurred while creating a TmNeuron" )
    })
    @PUT
    @Path("/workspace/neuron")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createTmNeuron(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                   @ApiParam @FormDataParam("neuronMetadata") TmNeuronMetadata neuron,
                                   @ApiParam @FormDataParam("protobufBytes") InputStream neuronPointsStream) {
        logger.debug("createTmNeuron({}, {})", subjectKey, neuron);
        TmWorkspace workspace = tmWorkspaceDao.findByIdAndSubjectKey(neuron.getWorkspaceId(), subjectKey);
        if (workspace != null) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Error getting the workspace for neuron " + neuron.toString()))
                    .build();
        } else {
            TmNeuronMetadata newNeuron = tmNeuronMetadataDao.createTmNeuronInWorkspace(subjectKey, neuron, workspace, neuronPointsStream);
            return Response.ok(newNeuron)
                    .build();
        }
    }

    @ApiOperation(value = "Updates existing neurons",
            notes = "Updates a list of neurons' metadata and ProtoBuf-serialized annotations"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully updated TmNeurons", response = List.class),
            @ApiResponse( code = 500, message = "Error occurred while updating TmNeurons" )
    })
    @POST
    @Path("/workspace/neuron")
    @Consumes(MultiPartMediaTypes.MULTIPART_MIXED)
    @Produces(MediaType.APPLICATION_JSON)
    public List<TmNeuronMetadata> updateTmNeurons(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                                  @ApiParam MultiPart multiPart) {
        int numParts = multiPart.getBodyParts().size();
        if (numParts % 2 != 0) {
            throw new IllegalArgumentException("Number of body parts is " + multiPart.getBodyParts().size() + " instead of a multiple of 2");
        }
        logger.trace("updateTmNeurons({}, numNeurons={})",subjectKey,numParts/2);
        List<TmNeuronMetadata> list = new ArrayList<>();
        for (int i = 0; i < numParts; i += 2) {
            BodyPart part0 = multiPart.getBodyParts().get(i);
            BodyPart part1 = multiPart.getBodyParts().get(i + 1);
            TmNeuronMetadata neuron = part0.getEntityAs(TmNeuronMetadata.class);
            InputStream protoBufStream = null;
            if (part1.getMediaType().equals(MediaType.APPLICATION_OCTET_STREAM_TYPE)) {
                BodyPartEntity bpe = (BodyPartEntity) part1.getEntity();
                protoBufStream = bpe.getInputStream();
            }
            TmNeuronMetadata updatedNeuron = tmNeuronMetadataDao.saveWithSubjectKey(neuron, subjectKey);
            tmNeuronMetadataDao.updateNeuronPoints(updatedNeuron, protoBufStream);
            list.add(updatedNeuron);
        }
        if (list.size() > 1) {
            logger.trace("{} updated {} neurons in workspace {}",
                    subjectKey, list.size(), list.stream().map(TmNeuronMetadata::getWorkspaceId).collect(Collectors.toSet()));
        } else if (list.size() == 1) {
            logger.trace("{} updated neuron {} in workspace {}",
                    subjectKey, list.get(0).getId(), list.get(0).getWorkspaceId());
        }
        return list;
    }

}

package org.janelia.jacs2.rest.sync.v2.dataresources;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.*;
import org.janelia.jacs2.cdi.qualifier.HortaSharedData;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.messaging.broker.neuronadapter.NeuronMessageHeaders;
import org.janelia.messaging.broker.neuronadapter.NeuronMessageType;
import org.janelia.messaging.core.MessageSender;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.domain.dao.TmNeuronMetadataDao;
import org.janelia.model.access.domain.dao.TmWorkspaceDao;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.tiledMicroscope.TmNeuronMetadata;
import org.janelia.model.domain.tiledMicroscope.TmWorkspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SwaggerDefinition(
        securityDefinition = @SecurityDefinition(
                apiKeyAuthDefinitions = {
                        @ApiKeyAuthDefinition(key = "user", name = "username", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER),
                        @ApiKeyAuthDefinition(key = "runAs", name = "runasuser", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER)
                }
        )
)
@Api(
        value = "Janelia Mouselight Shared Data Service",
        authorizations = {
                @Authorization("user"),
                @Authorization("runAs")
        }
)

@ApplicationScoped
@Produces("application/json")
@Path("/mouselight/data/shared")
public class TmSharedStateResource {

    private static final Logger LOG = LoggerFactory.getLogger(TmSharedStateResource.class);

    @AsyncIndex
    @Inject
    private TmWorkspaceDao tmWorkspaceDao;
    @Inject
    private TmNeuronMetadataDao tmNeuronMetadataDao;
    @HortaSharedData
    @Inject
    private MessageSender messageSender;

    @ApiOperation(value = "Creates a new neuron",
            notes = "Creates a neuron in the given workspace and notifies other users of the workspace"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully created a TmNeuron", response = TmNeuronMetadata.class),
            @ApiResponse(code = 500, message = "Error occurred while creating a TmNeuron")
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/neuron")
    public Response createTmNeuron(DomainQuery query) {
        TmNeuronMetadata neuron = query.getDomainObjectAs(TmNeuronMetadata.class);

        String subjectKey = query.getSubjectKey();
        LOG.info("createTmNeuron({}, workspaceId={}, name={})", subjectKey, neuron.getWorkspaceId(), neuron.getName());
        TmWorkspace workspace = getWorkspace(subjectKey, neuron.getWorkspaceId());

        try {
            // Create the neuron
            TmNeuronMetadata newNeuron = tmNeuronMetadataDao.createTmNeuronInWorkspace(subjectKey, neuron, workspace);
            // Notify other users
            sendMessage(subjectKey, neuron, NeuronMessageType.NEURON_CREATE, null);
            return Response.ok()
                    .entity(new GenericEntity<TmNeuronMetadata>(newNeuron){})
                    .build();
        }
        catch (Exception e) {
            LOG.error("Error creating neuron {}", neuron.getId(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error updating neuron " + neuron.getId()))
                    .build();
        }
        finally {
            LOG.trace("Finish createTmNeuron({}, workspaceId={}, name={})", subjectKey, neuron.getWorkspaceId(), neuron.getName());
        }
    }

    @ApiOperation(value = "Updates existing neurons",
            notes = "Updates a neuron and notifies other users of the workspace"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated TmNeurons", response = List.class),
            @ApiResponse(code = 500, message = "Error occurred while updating TmNeurons")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/neuron")
    public Response updateTmNeuron(DomainQuery query) {
        TmNeuronMetadata neuron = query.getDomainObjectAs(TmNeuronMetadata.class);
        String subjectKey = query.getSubjectKey();
        LOG.info("updateTmNeuron({}, workspaceId={}, neuronId={})", subjectKey, neuron.getWorkspaceId(), neuron.getId());

        try {
            TmWorkspace workspace = getWorkspace(subjectKey, neuron.getWorkspaceId());
            TmNeuronMetadata updatedNeuron = tmNeuronMetadataDao.saveNeuronMetadata(workspace, neuron,
                    subjectKey);
            // Notify other users
            sendMessage(subjectKey, neuron, NeuronMessageType.NEURON_SAVE_NEURONDATA, null);
            return Response.ok()
                    .entity(new GenericEntity<TmNeuronMetadata>(updatedNeuron){})
                    .build();
        }
        catch (Exception e) {
            LOG.error("Error updating neuron {}", neuron.getId(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error updating neuron " + neuron.getId()))
                    .build();
        }
        finally {
            LOG.trace("Finish updateTmNeuron({}, neuron={})", subjectKey, neuron);
        }
    }

    @ApiOperation(value = "Change neuron ownership",
            notes = "Updates a neuron to change its ownership and notifies other workspace users"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated TmNeuron", response = List.class),
            @ApiResponse(code = 500, message = "Error occurred while updating TmNeuron")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/neuron/ownership")
    public Response changeOwnership(DomainQuery query,
                                    @ApiParam @QueryParam("targetUser") final String targetUser) {
        TmNeuronMetadata neuron = query.getDomainObjectAs(TmNeuronMetadata.class);
        String subjectKey = query.getSubjectKey();

        LOG.info("changeOwnership({}, workspaceId={}, neuronId={}, target={})", subjectKey, neuron.getWorkspaceId(), neuron.getId(), targetUser);

        try {
            // Update the owner
            neuron.setOwnerKey(targetUser);
            neuron.getReaders().add(targetUser);
            neuron.getWriters().add(targetUser);

            // Save changes to database
            TmWorkspace workspace = getWorkspace(subjectKey, neuron.getWorkspaceId());
            TmNeuronMetadata updatedNeuron = tmNeuronMetadataDao.saveNeuronMetadata(workspace, neuron, subjectKey);

            // Notify other users
            Map<String, String> extraArgs = new HashMap<>();
            extraArgs.put(NeuronMessageHeaders.TARGET_USER, targetUser);
            sendMessage(subjectKey, neuron, NeuronMessageType.REQUEST_NEURON_ASSIGNMENT, extraArgs);

            return Response.ok()
                    .entity(new GenericEntity<TmNeuronMetadata>(updatedNeuron){})
                    .build();

        }
        catch (Exception e) {
            LOG.error("Error changing ownership of neuron {} to {}", neuron.getId(), targetUser, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error changing ownership of neuron " + neuron.getId()))
                    .build();
        }
        finally {
            LOG.trace("Finish changeOwnership({}, neuronId={}, target={})", subjectKey, neuron.getId(), targetUser);
        }
    }

    @ApiOperation(value = "Removes an existing neuron",
            notes = "Removes the neuron by its id and notifies other workspace users"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully removed a TmNeuron"),
            @ApiResponse(code = 500, message = "Error occurred while removing a TmNeuron")
    })
    @DELETE
    @Path("/workspace/neuron")
    public Response removeTmNeuron(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                               @ApiParam @QueryParam("workspaceId") final Long workspaceId,
                               @ApiParam @QueryParam("isLarge") final Boolean isLarge,
                               @ApiParam @QueryParam("neuronId") final Long neuronId) {

        LOG.info("removeTmNeuron({}, workspaceId={}, neuronId={})", subjectKey, workspaceId, neuronId);

        try {
            TmWorkspace workspace = getWorkspace(subjectKey, workspaceId);
            // TODO: remove unnecessary arguments to this function since we are fetching the neuron here
            TmNeuronMetadata neuron = tmNeuronMetadataDao.getTmNeuronMetadata(subjectKey, workspace, neuronId);
            if (!tmNeuronMetadataDao.removeTmNeuron(neuronId, isLarge, workspace, subjectKey)) {
                LOG.error("Error removing TM neuron {} in workspace {}: removeTmNeuron returned false", neuronId, workspaceId);
                return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(new ErrorResponse("Error removing neuron " + neuronId))
                        .build();
            }
            // Notify other users
            sendMessage(subjectKey, neuron, NeuronMessageType.NEURON_DELETE, null);
            return Response.ok().build();
        }
        catch (Exception e) {
            LOG.error("Error removing TM neuron {} in workspace {}", neuronId, workspaceId, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error removing neuron " + neuronId))
                    .build();
        }
        finally {
            LOG.trace("Finish removeTmNeuron({}, neuronId={}, workspaceId={})", subjectKey, neuronId, workspaceId);
        }
    }

    private TmWorkspace getWorkspace(String subjectKey, Long workspaceId) {
        TmWorkspace workspace = tmWorkspaceDao.findEntityByIdReadableBySubjectKey(workspaceId, subjectKey);
        if (workspace == null) {
            LOG.info("No workspace {} is accessible by {}", workspaceId, subjectKey);
            throw new WebApplicationException(Response.Status.BAD_REQUEST);
        }
        return workspace;
    }

    private void sendMessage(String subjectKey, TmNeuronMetadata neuron, NeuronMessageType type,
                             Map<String, String> extraArguments) throws Exception {
        // whatever the message is, unsync the object and increment the unsynced level counter
        neuron.setSynced(false);
        neuron.incrementSyncLevel();

        List<Long> neuronIds = new ArrayList<>();
        neuronIds.add(neuron.getId());
        ObjectMapper mapper = new ObjectMapper();
        byte[] neuronData = mapper.writeValueAsBytes(neuron);

        Map<String, Object> updateHeaders = new HashMap<>();
        updateHeaders.put(NeuronMessageHeaders.TYPE, type.toString());
        updateHeaders.put(NeuronMessageHeaders.USER, subjectKey);
        updateHeaders.put(NeuronMessageHeaders.WORKSPACE, neuron.getWorkspaceId().toString());
        updateHeaders.put(NeuronMessageHeaders.NEURONIDS, neuronIds.toString());
        if (extraArguments != null) {
            for (String extraKey : extraArguments.keySet()) {
                updateHeaders.put(extraKey, extraArguments.get(extraKey));
            }
        }

        messageSender.sendMessage(updateHeaders, neuronData);
    }
}

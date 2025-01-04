package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.GenericEntity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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

@Tag(name = "TmSharedState",
        description = "Janelia Mouselight Shared Data Service"
)
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

    @Operation(summary = "Creates a new neuron",
            description = "Creates a neuron in the given workspace and notifies other users of the workspace"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully created a TmNeuron"),
            @ApiResponse(responseCode = "500", description = "Error occurred while creating a TmNeuron")
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
                    .entity(new GenericEntity<TmNeuronMetadata>(newNeuron) {
                    })
                    .build();
        } catch (Exception e) {
            LOG.error("Error creating neuron {}", neuron.getId(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error updating neuron " + neuron.getId()))
                    .build();
        } finally {
            LOG.trace("Finish createTmNeuron({}, workspaceId={}, name={})", subjectKey, neuron.getWorkspaceId(), neuron.getName());
        }
    }

    @Operation(summary = "Updates existing neurons",
            description = "Updates a neuron and notifies other users of the workspace"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated TmNeurons"),
            @ApiResponse(responseCode = "500", description = "Error occurred while updating TmNeurons")
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
                    .entity(new GenericEntity<TmNeuronMetadata>(updatedNeuron) {
                    })
                    .build();
        } catch (Exception e) {
            LOG.error("Error updating neuron {}", neuron.getId(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error updating neuron " + neuron.getId()))
                    .build();
        } finally {
            LOG.trace("Finish updateTmNeuron({}, neuron={})", subjectKey, neuron);
        }
    }

    @Operation(summary = "Change neuron ownership",
            description = "Updates a neuron to change its ownership and notifies other workspace users"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated TmNeuron"),
            @ApiResponse(responseCode = "500", description = "Error occurred while updating TmNeuron")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/workspace/neuron/ownership")
    public Response changeOwnership(DomainQuery query,
                                    @Parameter @QueryParam("targetUser") final String targetUser) {
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
                    .entity(new GenericEntity<TmNeuronMetadata>(updatedNeuron) {
                    })
                    .build();

        } catch (Exception e) {
            LOG.error("Error changing ownership of neuron {} to {}", neuron.getId(), targetUser, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error changing ownership of neuron " + neuron.getId()))
                    .build();
        } finally {
            LOG.trace("Finish changeOwnership({}, neuronId={}, target={})", subjectKey, neuron.getId(), targetUser);
        }
    }

    @Operation(summary = "Removes an existing neuron",
            description = "Removes the neuron by its id and notifies other workspace users"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully removed a TmNeuron"),
            @ApiResponse(responseCode = "500", description = "Error occurred while removing a TmNeuron")
    })
    @DELETE
    @Path("/workspace/neuron")
    public Response removeTmNeuron(@Parameter @QueryParam("subjectKey") final String subjectKey,
                                   @Parameter @QueryParam("workspaceId") final Long workspaceId,
                                   @Parameter @QueryParam("isLarge") final Boolean isLarge,
                                   @Parameter @QueryParam("neuronId") final Long neuronId) {

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
        } catch (Exception e) {
            LOG.error("Error removing TM neuron {} in workspace {}", neuronId, workspaceId, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error removing neuron " + neuronId))
                    .build();
        } finally {
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

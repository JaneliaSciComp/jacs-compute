package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.Collections;
import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
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
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.NodeDao;
import org.janelia.model.access.domain.dao.WorkspaceNodeDao;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.workspace.Node;
import org.janelia.model.domain.workspace.TreeNode;
import org.janelia.model.domain.workspace.Workspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Web service for handling Node entities.
 */
@Tag(name = "DataNode",
        description = "Data Node Service"
)
@RequireAuthentication
@Path("/data")
public class DataNodeResource {
    private static final Logger LOG = LoggerFactory.getLogger(DataNodeResource.class);

    @Inject
    private LegacyDomainDao legacyDomainDao;
    @AsyncIndex
    @Inject
    private WorkspaceNodeDao workspaceNodeDao;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{nodeId}")
    public Response getDataNode(@PathParam("nodeId") Long dataNodeId,
                                @Context ContainerRequestContext containerRequestContext) {
        String authorizedSubjectKey = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
        Node dataNode = legacyDomainDao.getDomainObject(authorizedSubjectKey, Node.class, dataNodeId);
        if (dataNode == null) {
            LOG.warn("No folder found for {} owned by {}", dataNodeId, authorizedSubjectKey);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
        return Response
                .ok(dataNode)
                .build();
    }

    @Operation(summary = "Gets all the children of a node",
            description = "Returns all the children of the given node which are visible to the current user."
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully got children"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error getting node children" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/node/children")
    public Response getNodeChildren(@Parameter @QueryParam("subjectKey") String subjectKey,
                                    @Parameter @QueryParam("nodeRef") String nodeReference,
                                    @Parameter @QueryParam("sortCriteria") String sortCriteria,
                                    @Parameter @QueryParam("page") int page,
                                    @Parameter @QueryParam("pageSize") int pageSize) {
        LOG.trace("Start getNodeChildren({}, {}, {}, {}, {})", subjectKey, nodeReference, sortCriteria, page, pageSize);
        try {
            if (StringUtils.isBlank(subjectKey)) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("Invalid subject key"))
                        .build();
            }

            Node node = (Node)legacyDomainDao.getDomainObject(subjectKey, Reference.createFor(nodeReference));
            if (node==null) {
                return Response.ok()
                        .type(MediaType.APPLICATION_JSON)
                        .entity(new GenericEntity<List<DomainObject>>(Collections.emptyList()){})
                        .build();
            }

            List<DomainObject> children = workspaceNodeDao.getChildren(subjectKey, node, sortCriteria, page, pageSize);
            LOG.trace("Found {} children accessible by {}", children.size(), subjectKey);
            return Response.ok()
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new GenericEntity<List<DomainObject>>(children){})
                    .build();
        } finally {
            LOG.trace("Finished getNodeChildren({}, {}, {}, {}, {})", subjectKey, nodeReference, sortCriteria, page, pageSize);
        }
    }

    @Operation(summary = "Adds items to a Node",
            description = "Uses the DomainObject parameter of the DomainQuery for the Node, " +
                    "the References parameter for the list of items to add"
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully added items to the Node"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error adding items to the Node" )
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/node/children")
    @SuppressWarnings("unchecked")
    public <T extends Node> Response addChildren(@Parameter DomainQuery query,
                                                 @Context ContainerRequestContext containerRequestContext) {
        LOG.trace("addChildren({})",query);
        String authorizedSubjectKey = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
        String subjectKey;
        if (StringUtils.isBlank(authorizedSubjectKey)) {
            subjectKey = query.getSubjectKey();
        } else {
            subjectKey = authorizedSubjectKey;
        }
        try {
            T parentNode = (T) query.getDomainObjectAs(Node.class);
            T existingParentNode = (T) legacyDomainDao.getDomainObject(subjectKey, Reference.createFor(parentNode));
            if (existingParentNode == null) {
                LOG.warn("No folder found for parent node {} accessible by {}", parentNode, subjectKey);
                return Response
                        .status(Response.Status.NOT_FOUND)
                        .build();
            }
            Node updatedNode = legacyDomainDao.addChildren(subjectKey, existingParentNode, query.getReferences());
            return Response
                    .ok()
                    .entity(updatedNode)
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred in add children with {}",query, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error while trying to add children: " + query))
                    .build();
        } finally {
            LOG.trace("Finished addChildren({})", query);
        }
    }

    @Operation(summary = "Removes items from a Node",
            description = "Uses the DomainObject parameter of the DomainQuery for the Node, " +
                    "the References parameter for the list of items to remove"
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully removed items from the Node"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error removing items from the Node" )
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/node/children")
    @SuppressWarnings("unchecked")
    public <T extends Node> Response removeChildren(@Parameter DomainQuery query,
                                                    @Context ContainerRequestContext containerRequestContext) {
        LOG.trace("Start removeChildren({})",query);
        String authorizedSubjectKey = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
        String subjectKey;
        if (StringUtils.isBlank(authorizedSubjectKey)) {
            subjectKey = query.getSubjectKey();
        } else {
            subjectKey = authorizedSubjectKey;
        }
        try {
            T parentNode = (T) query.getDomainObjectAs(Node.class);
            T existingParentNode = (T) legacyDomainDao.getDomainObject(subjectKey, Reference.createFor(parentNode));
            if (existingParentNode == null) {
                LOG.warn("No folder found for parent node {} accessible by {}", parentNode, subjectKey);
                return Response
                        .status(Response.Status.NO_CONTENT)
                        .build();
            }
            Node updatedNode = legacyDomainDao.removeChildren(subjectKey, existingParentNode, query.getReferences());
            return Response
                    .status(Response.Status.OK)
                    .entity(updatedNode)
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred in remove children with {}",query, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error while trying remove children nodes using " + query))
                    .build();
        } finally {
            LOG.trace("Finish removeChildren({})",query);
        }
    }

    @Operation(summary = "Reorders the items in a node",
            description = "Uses the DomainObject parameter of the DomainQuery and the Ordering parameter for the new ordering."
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully reordered Node"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error reordering Node" )
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/node/reorder")
    public Response reorderNode(@Parameter DomainQuery query,
                                @Context ContainerRequestContext containerRequestContext) {
        LOG.trace("Start reorderNode({})",query);
        String authorizedSubjectKey = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
        String subjectKey;
        if (StringUtils.isBlank(authorizedSubjectKey)) {
            subjectKey = query.getSubjectKey();
        } else {
            subjectKey = authorizedSubjectKey;
        }
        try {
            List<Integer> orderList = query.getOrdering();
            int[] order = new int[orderList.size()];
            for (int i=0; i < orderList.size(); i++) {
                order[i] = orderList.get(i).intValue();
            }
            Node parentNode = query.getDomainObjectAs(Node.class);
            Node updatedNode = legacyDomainDao.reorderChildren(subjectKey, parentNode, order);
            return Response
                    .ok()
                    .entity(updatedNode)
                    .build();

        } catch (Exception e) {
            LOG.error("Error occurred in reorder nodes with {}",query, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error while trying to reorder nodes: " + query))
                    .build();
        } finally {
            LOG.trace("Finish reorderNode({})",query);
        }
    }

    @Operation(summary = "Creates a folder",
            description = "Uses the DomainObject parameter of the DomainQuery to create a new TreeNode (folder) object."
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully creating TreeNode"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error creating TreeNode" )
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("treenode")
    @SuppressWarnings("unchecked")
    public <T extends TreeNode> Response createTreeNode(@Parameter DomainQuery query) {
        LOG.trace("Start createTreeNode({})", query);
        try {
            T dn = (T) query.getDomainObjectAs(TreeNode.class);
            T savedNode = ((NodeDao<T>)workspaceNodeDao).saveBySubjectKey(dn, query.getSubjectKey());
            return Response.created(UriBuilder.fromMethod(DataNodeResource.class, "getDataNode").build(savedNode.getId()))
                    .entity(savedNode)
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred creating tree node for {}", query, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error while trying to create a treeNode from " + query))
                    .build();
        } finally {
            LOG.trace("Finished createTreeNode({})", query);
        }
    }

    @Operation(summary = "Gets the user's default Workspace",
            description = "Returns the user's default Workspace object."
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully got default workspace"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error getting default workspace" )
    })
    @GET
    @Path("workspace")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDefaultWorkspaceBySubjectKey(@Parameter @QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getDefaultWorkspaceBySubjectKey({})", subjectKey);
        try {
            if (StringUtils.isBlank(subjectKey)) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("Invalid subject key"))
                        .build();
            }
            List<Workspace> subjectOwnedWorkspaces = workspaceNodeDao.getWorkspaceNodesOwnedBySubjectKey(subjectKey);
            return subjectOwnedWorkspaces.stream()
                    .findFirst()
                    .map(ws -> {
                        LOG.debug("First workspace found owned by {} is {}", subjectKey, ws.getId());
                        return Response.ok()
                                .entity(ws)
                                .build();

                    })
                    .orElseGet(() -> {
                        LOG.warn("No workspace found for {}", subjectKey);
                        return Response.status(Response.Status.BAD_REQUEST)
                                .entity(new ErrorResponse("No workspace found for " + subjectKey))
                                .build();
                    });
        } finally {
            LOG.trace("Finished getDefaultWorkspaceBySubjectKey({})", subjectKey);
        }
    }

    @Operation(summary = "Gets all the Workspaces a user can read",
            description = "Returns all the Workspaces which are visible to the current user."
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully got all workspaces"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error getting workspaces" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("workspaces")
    public Response getAllWorkspacesBySubjectKey(@Parameter @QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getAllWorkspacesBySubjectKey({})", subjectKey);
        try {
            if (StringUtils.isBlank(subjectKey)) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("Invalid subject key"))
                        .build();
            }
            List<Workspace> allAccessibleWorkspaces = workspaceNodeDao.getWorkspaceNodesAccessibleBySubjectGroups(subjectKey, 0L, -1);
            LOG.debug("Found {} accessible workspaces by {}", allAccessibleWorkspaces.size(), subjectKey);
            return Response.ok()
                    .type(MediaType.APPLICATION_JSON)
                    .entity(new GenericEntity<List<Workspace>>(allAccessibleWorkspaces){})
                    .build();
        } finally {
            LOG.trace("Finished getAllWorkspacesBySubjectKey({})", subjectKey);
        }
    }

}

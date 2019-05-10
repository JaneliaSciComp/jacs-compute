package org.janelia.jacs2.rest.sync.v2.dataresources;

import com.google.common.collect.ImmutableList;
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
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.NodeDao;
import org.janelia.model.access.domain.dao.WorkspaceNodeDao;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.workspace.TreeNode;
import org.janelia.model.domain.workspace.Workspace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
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
import javax.ws.rs.core.UriBuilder;
import java.util.List;

/**
 * Web service for handling Workspace and TreeNode entities.
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
        value = "Data TreeNode and Workspace Service",
        authorizations = {
                @Authorization("user"),
                @Authorization("runAs")
        }
)
@RequireAuthentication
@ApplicationScoped
@Path("/data")
public class DataTreeNodeResource {

    private static final Logger LOG = LoggerFactory.getLogger(DataTreeNodeResource.class);

    @Inject
    private LegacyDomainDao legacyFolderDao;
    @Inject
    private WorkspaceNodeDao workspaceNodeDao;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{nodeId}")
    public Response getDataNode(@PathParam("nodeId") Long dataNodeId,
                                @Context ContainerRequestContext containerRequestContext) {
        String authorizedSubjectKey = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
        TreeNode dataNode = legacyFolderDao.getDomainObject(authorizedSubjectKey, TreeNode.class, dataNodeId);
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

    @ApiOperation(value = "Adds items to a Node",
            notes = "Uses the DomainObject parameter of the DomainQuery for the Node, " +
                    "the References parameter for the list of items to add"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully added items to the Node", response=TreeNode.class),
            @ApiResponse( code = 500, message = "Internal Server Error adding items to the Node" )
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/node/children")
    @SuppressWarnings("unchecked")
    public <T extends TreeNode> Response addChildren(@ApiParam DomainQuery query,
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
            T parentNode = (T) query.getDomainObjectAs(TreeNode.class);
            TreeNode existingParentNode = legacyFolderDao.getDomainObject(subjectKey, TreeNode.class, parentNode.getId());
            if (existingParentNode == null) {
                LOG.warn("No folder found for parent node {} accessible by {}", parentNode, subjectKey);
                return Response
                        .status(Response.Status.NOT_FOUND)
                        .build();
            }
            TreeNode updatedNode = legacyFolderDao.addChildren(subjectKey, existingParentNode, query.getReferences());
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
            LOG.trace("Finished createTreeNode({})", query);
        }
    }

    @ApiOperation(value = "Removes items from a Node",
            notes = "Uses the DomainObject parameter of the DomainQuery for the Node, " +
                    "the References parameter for the list of items to remove"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully removed items from the Node", response = TreeNode.class),
            @ApiResponse( code = 500, message = "Internal Server Error removing items from the Node" )
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/node/children")
    @SuppressWarnings("unchecked")
    public <T extends TreeNode> Response removeChildren(@ApiParam DomainQuery query,
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
            T parentNode = (T) query.getDomainObjectAs(TreeNode.class);
            TreeNode existingParentNode = legacyFolderDao.getDomainObject(subjectKey, TreeNode.class, parentNode.getId());
            if (existingParentNode == null) {
                LOG.warn("No folder found for parent node {} accessible by {}", parentNode, subjectKey);
                return Response
                        .status(Response.Status.NO_CONTENT)
                        .build();
            }
            TreeNode updatedNode = legacyFolderDao.removeChildren(subjectKey, existingParentNode, query.getReferences());
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

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/node/{nodeId}/children/{folder}")
    public Response addChildrenNodes(@PathParam("nodeId") Long dataNodeId,
                                     @PathParam("folder") String folderName,
                                     @Context ContainerRequestContext containerRequestContext) {
        LOG.trace("Start addChildren({}, {})", dataNodeId, folderName);
        String authorizedSubjectKey = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
        TreeNode parentNode = legacyFolderDao.getDomainObject(authorizedSubjectKey, TreeNode.class, dataNodeId);
        if (parentNode == null) {
            LOG.warn("No folder found for {} owned by {}", dataNodeId, authorizedSubjectKey);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
        try {
            TreeNode folder = new TreeNode();
            folder.setName(folderName);
            TreeNode newFolder = legacyFolderDao.save(authorizedSubjectKey, folder);
            legacyFolderDao.addChildren(authorizedSubjectKey, parentNode, ImmutableList.of(Reference.createFor(newFolder)));
            return Response
                    .status(Response.Status.CREATED)
                    .entity(newFolder)
                    .contentLocation(UriBuilder.fromMethod(DataTreeNodeResource.class, "getDataNode").build(newFolder.getId()))
                    .build();
        } catch (Exception e) {
            LOG.error("Error while trying to add child folder {} to {}", folderName, parentNode, e);
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .build();
        } finally {
            LOG.trace("Finished addChildren({}, {})", dataNodeId, folderName);
        }
    }

    @ApiOperation(value = "Reorders the items in a node",
            notes = "Uses the DomainObject parameter of the DomainQuery and the Ordering parameter for the new ordering."
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully reordered Node", response = TreeNode.class),
            @ApiResponse( code = 500, message = "Internal Server Error reordering Node" )
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/node/reorder")
    public Response reorderNode(@ApiParam DomainQuery query,
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
            TreeNode parentNode = query.getDomainObjectAs(TreeNode.class);
            TreeNode updatedNode = legacyFolderDao.reorderChildren(subjectKey, parentNode, order);
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

    @ApiOperation(value = "Creates a folder",
            notes = "Uses the DomainObject parameter of the DomainQuery to create a new TreeNode (folder) object."
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully creating TreeNode", response=TreeNode.class),
            @ApiResponse( code = 500, message = "Internal Server Error creating TreeNode" )
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("treenode")
    @SuppressWarnings("unchecked")
    public <T extends TreeNode> Response createTreeNode(@ApiParam DomainQuery query) {
        LOG.trace("Start createTreeNode({})", query);
        try {
            T dn = (T) query.getDomainObjectAs(TreeNode.class);
            T savedNode = ((NodeDao<T>)workspaceNodeDao).saveBySubjectKey(dn, query.getSubjectKey());
            return Response.created(UriBuilder.fromMethod(DataTreeNodeResource.class, "getDataNode").build(savedNode.getId()))
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

    @ApiOperation(value = "Gets the user's default Workspace",
            notes = "Returns the user's default Workspace object."
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully got default workspace", response= Workspace.class),
            @ApiResponse( code = 500, message = "Internal Server Error getting default workspace" )
    })
    @GET
    @Path("workspace")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDefaultWorkspaceBySubjectKey(@ApiParam @QueryParam("subjectKey") String subjectKey) {
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

    @ApiOperation(value = "Gets all the Workspaces a user can read",
            notes = "Returns all the Workspaces which are visible to the current user."
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully got all workspaces", response=Workspace.class,
                    responseContainer =  "List"),
            @ApiResponse( code = 500, message = "Internal Server Error getting workspaces" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("workspaces")
    public Response getAllWorkspacesBySubjectKey(@ApiParam @QueryParam("subjectKey") String subjectKey) {
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

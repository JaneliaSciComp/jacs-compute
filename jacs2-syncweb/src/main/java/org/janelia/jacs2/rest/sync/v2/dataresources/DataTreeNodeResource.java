package org.janelia.jacs2.rest.sync.v2.dataresources;

import com.google.common.collect.ImmutableList;
import org.glassfish.jersey.server.ContainerRequest;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.workspace.TreeNode;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

@RequireAuthentication
@ApplicationScoped
@Produces("application/json")
@Path("/data")
public class DataTreeNodeResource {

    @Inject private LegacyDomainDao folderDao;
    @Inject private Logger logger;

    @GET
    @Path("{node-id}")
    public Response getDataNode(@PathParam("node-id") Long dataNodeId,
                                @Context ContainerRequest containerRequestContext) {
        String authorizedSubjectKey = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
        TreeNode dataNode = folderDao.getDomainObject(authorizedSubjectKey, TreeNode.class, dataNodeId);
        if (dataNode == null) {
            logger.warn("No folder found for {} owned by {}", dataNodeId, authorizedSubjectKey);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
        return Response
                .ok(dataNode)
                .build();
    }

    @PUT
    @Path("/node/{node-id}/children/{folder}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addChildrenNodes(@PathParam("node-id") Long dataNodeId,
                                     @PathParam("folder") String folderName,
                                     @Context ContainerRequest containerRequestContext) {
        String authorizedSubjectKey = JacsSecurityContextHelper.getAuthorizedSubjectKey(containerRequestContext);
        TreeNode parentFolder = folderDao.getDomainObject(authorizedSubjectKey, TreeNode.class, dataNodeId);
        if (parentFolder == null) {
            logger.warn("No folder found for {} owned by {}", dataNodeId, authorizedSubjectKey);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
        try {
            TreeNode folder = new TreeNode();
            folder.setName(folderName);
            TreeNode newFolder = folderDao.save(authorizedSubjectKey, folder);
            folderDao.addChildren(authorizedSubjectKey, parentFolder, ImmutableList.of(Reference.createFor(newFolder)));

            return Response
                    .status(Response.Status.CREATED)
                    .entity(newFolder)
                    .contentLocation(UriBuilder.fromMethod(DataTreeNodeResource.class, "getDataNode").build(newFolder.getId()))
                    .build();
        } catch (Exception e) {
            logger.error("Error while trying to add child folder {} to {}", folderName, parentFolder, e);
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .build();
        }
    }
}

package org.janelia.jacs2.rest.sync.v2.dataresources;

import com.google.common.collect.ImmutableSet;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.DomainUtils;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.ReverseReference;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.gui.colordepth.ColorDepthMask;
import org.janelia.model.domain.gui.colordepth.ColorDepthSearch;
import org.janelia.model.domain.gui.search.Filter;
import org.janelia.model.domain.sample.Image;
import org.janelia.model.domain.workspace.GroupedFolder;
import org.janelia.model.domain.workspace.TreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@SwaggerDefinition(
        securityDefinition = @SecurityDefinition(
                apiKeyAuthDefinitions = {
                        @ApiKeyAuthDefinition(key = "user", name = "username", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER),
                        @ApiKeyAuthDefinition(key = "runAs", name = "runasuser", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER)
                }
        )
)
@Api(
        value = "Janelia Workstation Domain Data",
        authorizations = {
                @Authorization("user"),
                @Authorization("runAs")
        }
)
@RequireAuthentication
@Path("/data")
public class DomainObjectResource {
    private static final Logger LOG = LoggerFactory.getLogger(DomainObjectResource.class);

    @Inject
    private LegacyDomainDao legacyDomainDao;

    @ApiOperation(value = "Updates an Object's Attribute",
            notes = "uses the ObjectType, ObjectId(first position), PropertyName, and PropertyValue parameters of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully updated a domain object's properties", response=DomainObject.class),
            @ApiResponse( code = 500, message = "Internal Server Error updating a domain object's properties" )
    })
    @POST
    @Path("/domainobject")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateObjectProperty(@ApiParam DomainQuery query) {
        LOG.trace("Start updateObjectProperty({})", query);
        try {
            DomainObject updateObj;
            List<Long> objIds = query.getObjectIds();
            if (CollectionUtils.isNotEmpty(objIds)) {
                Class<? extends DomainObject> domainObjectClass = DomainUtils.getObjectClassByName(query.getObjectType());
                DomainObject currObj = legacyDomainDao.getDomainObject(query.getSubjectKey(), domainObjectClass, objIds.get(0));
                updateObj = legacyDomainDao.updateProperty(query.getSubjectKey(), domainObjectClass, currObj.getId(),
                        query.getPropertyName(), query.getPropertyValue());
                return Response.ok(updateObj)
                        .build();
            } else {
                LOG.info("Empty objectIds in {}", query);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("No ID has been specified"))
                        .build();
            }
        } catch (Exception e) {
            LOG.error("Error occurred processing Domain Object Update Property {}", query, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished updateObjectProperty({})", query);
        }
    }

    @ApiOperation(value = "creates or updates a DomainObject ",
            notes = "uses the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully created/updated the value of an existing Domain Object", response=DomainObject.class),
            @ApiResponse( code = 500, message = "Internal Server Error updating DomainObject" )
    })
    @PUT
    @Path("/domainobject")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public DomainObject saveDomainObject(@ApiParam DomainQuery query) {
        LOG.trace("Start saveDomainObject({})", query);
        try {
            return legacyDomainDao.save(query.getSubjectKey(), query.getDomainObject());
        } catch (Exception e) {
            LOG.error("Error occurred updating Domain Object {}", query, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished saveDomainObject({})", query);
        }
    }

    @ApiOperation(value = "Gets an Domain Object's Details using either the references parameters or the objectType & objectIds")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got a list of DomainObjectst", response=DomainObject.class, responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting list of DomainObjects" )
    })
    @POST
    @Path("/domainobject/details")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getObjectDetails(@ApiParam DomainQuery query) {
        LOG.trace("Start getObjectDetails(({})", query);
        try {
            List<DomainObject> detailObjects;
            if (query.getReferences() != null) {
                detailObjects = legacyDomainDao.getDomainObjects(query.getSubjectKey(), query.getReferences());
            } else if (query.getObjectIds() != null) {
                detailObjects = legacyDomainDao.getDomainObjects(query.getSubjectKey(), query.getObjectType(), query.getObjectIds());
            } else {
                detailObjects = Collections.emptyList();
            }
            return Response.ok()
                    .entity(new GenericEntity<List<DomainObject>>(detailObjects){})
                    .build();
        } finally {
            LOG.trace("Finished getObjectDetails(({})", query);
        }
    }

    /**
     * Use for getting all examples of a given object type.  Only for small sets.
     * @param subjectKey constrains by ownership.
     * @param domainClass required. constrains by collection/type.
     * @return all existing examples of things of this type.
     */
    @GET
    @Path("/domainobject/class")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getObjectsByClass(@QueryParam("subjectKey") final String subjectKey,
                                      @QueryParam("domainClass") final String domainClass) {
        LOG.trace("Start getObjectsByClass({}, domainClass={})", subjectKey, domainClass);
        Class<? extends DomainObject> domainObjectClass;
        try {
            domainObjectClass = DomainUtils.getObjectClassByName(domainClass);
        } catch (Exception e) {
            LOG.error("Error getting domain object class for {}", domainClass, e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("Invalid domain class " + domainClass))
                    .build();
        }
        try {
            List<? extends DomainObject> domainObjects = legacyDomainDao.getDomainObjects(subjectKey, domainObjectClass);
            return Response.ok()
                    .entity(new GenericEntity<List<? extends DomainObject>>(domainObjects){})
                    .build();
        } finally {
            LOG.trace("Finished getObjectsByClass({}, domainClass={})", subjectKey, domainClass);
        }
    }

    @ApiOperation(value = "Gets DomainObjects by Name and DomainClass")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully got a list of DomainObjects", response = DomainObject.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error getting list of DomainObjects")
    })
    @GET
    @Path("/domainobject/name")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getObjectsByName(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                               @ApiParam @QueryParam("name") final String name,
                                               @ApiParam @QueryParam("domainClass") final String domainClass) {
        LOG.trace("Start getObjectsByName({}, name={}, domainClass={})", subjectKey, name, domainClass);
        Class<? extends DomainObject> clazz = DomainUtils.getObjectClassByName(domainClass);
        try {
            List<? extends DomainObject> domainObjects = legacyDomainDao.getDomainObjectsByName(subjectKey, clazz, name);
            return Response.ok()
                    .entity(new GenericEntity<List<? extends DomainObject>>(domainObjects){})
                    .build();
        } finally {
            LOG.trace("Finished getObjectsByName({}, name={}, domainClass={})", subjectKey, name, domainClass);
        }
    }

    @ApiOperation(value = "Removes a Domain Object",
            notes = "uses the References parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully removed all domain objects"),
            @ApiResponse( code = 500, message = "Internal Server Error removing domain objects" )
    })
    @POST
    @Path("/domainobject/remove")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeDomainObject(@ApiParam DomainQuery query) {
        LOG.trace("Start removeDomainObject({})", query);
        try {
            List<ErrorResponse> permissionErrors = new ArrayList<>();
            for (Reference objectRef : query.getReferences()) {
                // first check that it is a treeNode
                Class<? extends DomainObject> objClass = DomainUtils.getObjectClassByName(objectRef.getTargetClassName());
                if (canBeDeleted(objClass)) {
                    String subjectKey = query.getSubjectKey();
                    DomainObject domainObj = legacyDomainDao.getDomainObject(subjectKey, objectRef);
                    // check whether this subject has permissions to write to this object
                    if (domainObj.getWriters().contains(subjectKey)) {
                        legacyDomainDao.deleteDomainObject(subjectKey, objClass, domainObj.getId());
                    } else {
                        LOG.warn("Attempt to remove {} by {} without write permissions", domainObj, subjectKey);
                        permissionErrors.add(new ErrorResponse("User " + subjectKey + " has no write permissions on " + domainObj.getId()));
                    }
                } else {
                    LOG.warn("This API cannot delete this type of object: {}", objectRef);
                    permissionErrors.add(new ErrorResponse("Objects of type " + objClass + " are not removable"));
                }
            }
            if (permissionErrors.isEmpty()) {
                return Response.noContent()
                        .build();
            } else {
                return Response.status(Response.Status.FORBIDDEN)
                        .entity(permissionErrors)
                        .build();
            }
        } catch (Exception e) {
            LOG.error("Error occurred removing object references for {}", query, e);
            throw new WebApplicationException(Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            LOG.trace("Finished removeDomainObject({})", query);
        }
    }

    private boolean canBeDeleted(Class<? extends DomainObject> objectType) {
        final Set<Class<? extends DomainObject>> deletableDomainTypes = ImmutableSet.of(
                TreeNode.class,
                Filter.class,
                ColorDepthSearch.class,
                ColorDepthMask.class,
                GroupedFolder.class,
                Image.class

        );
        return deletableDomainTypes.contains(objectType);
    }

    @ApiOperation(value = "Gets a list of DomainObjects that are referring to this DomainObject",
            notes = "Uses reference attribute and reference class to determine type of parent reference to find"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully got a list of DomainObjectst", response=DomainObject.class,
                    responseContainer = "List"),
            @ApiResponse( code = 500, message = "Internal Server Error getting list of DomainObjects" )
    })
    @GET
    @Path("/domainobject/reverseLookup")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getObjectsByReverseRef(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                                     @ApiParam @QueryParam("referenceId") final Long referenceId,
                                                     @ApiParam @QueryParam("count") final Long count,
                                                     @ApiParam @QueryParam("referenceAttr") final String referenceAttr,
                                                     @ApiParam @QueryParam("referenceClass") final String referenceClass) {
        LOG.trace("Start getObjectsByReverseRef({}, referenceId={}, count={}, referenceAttr={}, referenceClass={})", subjectKey, referenceId, count, referenceAttr, referenceClass);
        ReverseReference reverseRef = new ReverseReference();
        reverseRef.setCount(count);
        reverseRef.setReferenceAttr(referenceAttr);
        reverseRef.setReferenceId(referenceId);
        reverseRef.setReferringClassName(referenceClass);
        try {
            List<? extends DomainObject> domainObjects = legacyDomainDao.getDomainObjects(subjectKey, reverseRef);
            return Response.ok()
                    .entity(new GenericEntity<List<? extends DomainObject>>(domainObjects){})
                    .build();
        } finally {
            LOG.trace("Finished getObjectsByReverseRef({}, referenceId={}, count={}, referenceAttr={}, referenceClass={})", subjectKey, referenceId, count, referenceAttr, referenceClass);
        }
    }

}

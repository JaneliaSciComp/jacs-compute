package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.GenericEntity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.google.common.collect.ImmutableSet;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.domain.DomainUtils;
import org.janelia.model.domain.Reference;
import org.janelia.model.domain.ReverseReference;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.gui.cdmip.ColorDepthMask;
import org.janelia.model.domain.gui.cdmip.ColorDepthSearch;
import org.janelia.model.domain.gui.search.Filter;
import org.janelia.model.domain.sample.Image;
import org.janelia.model.domain.workspace.GroupedFolder;
import org.janelia.model.domain.workspace.TreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "DomainObject", description = "Janelia Workstation Domain Data")
@RequireAuthentication
@Path("/data")
public class DomainObjectResource {
    private static final Logger LOG = LoggerFactory.getLogger(DomainObjectResource.class);

    @Inject
    private LegacyDomainDao legacyDomainDao;

    @Operation(summary = "Updates an Object's Attribute",
            description = "uses the ObjectType, ObjectId(first position), PropertyName, and PropertyValue parameters of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully updated a domain object's properties"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error updating a domain object's properties" )
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/domainobject")
    public Response updateObjectProperty(@Parameter DomainQuery query) {
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

    @Operation(summary = "creates or updates a DomainObject ",
            description = "uses the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully created/updated the value of an existing Domain Object"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error updating DomainObject" )
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/domainobject")
    public DomainObject saveDomainObject(@Parameter DomainQuery query) {
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

    @Operation(summary = "Gets an Domain Object's Details using either the references parameters or the objectType & objectIds")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got a list of DomainObjectst"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting list of DomainObjects" )
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/domainobject/details")
    public Response getObjectDetails(@Parameter DomainQuery query) {
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
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/domainobject/class")
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

    @Operation(summary = "Gets DomainObjects by Name and DomainClass")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got a list of DomainObjects"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting list of DomainObjects")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/domainobject/name")
    public Response getObjectsByName(@Parameter @QueryParam("subjectKey") final String subjectKey,
                                     @Parameter @QueryParam("name") final String name,
                                     @Parameter @QueryParam("domainClass") final String domainClass) {
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

    @Operation(summary = "Gets DomainObjects by Property Value")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got a list of DomainObjects"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting list of DomainObjects")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/domainobject/withproperty")
    public Response getObjectsWithProperty(@Parameter DomainQuery query) {
        LOG.trace("Start getObjectsByProperty({})", query);
        Class<? extends DomainObject> clazz = DomainUtils.getObjectClassByName(query.getObjectType());
        try {
            List<? extends DomainObject> domainObjects = legacyDomainDao.getDomainObjectsWithProperty(query.getSubjectKey(), clazz, query.getPropertyName(), query.getPropertyValue());
            return Response.ok()
                    .entity(new GenericEntity<List<? extends DomainObject>>(domainObjects){})
                    .build();
        } finally {
            LOG.trace("Finished getObjectsByProperty({})", query);
        }
    }

    @Operation(
            summary = "Gets Folder References to a DomainObject ",
            description = "uses the DomainObject parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Successfully got a list of Folder References"),
            @ApiResponse(
                    responseCode = "500",
                    description = "Internal Server Error getting list of Folder References" )
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/domainobject/references")
    public Response getContainerReferences(@Parameter DomainQuery query) {
        LOG.trace("Start getContainerReferences({})", query);
        try {
            List<Reference> objectReferences = legacyDomainDao.getAllNodeContainerReferences(query.getDomainObject());
            return Response.ok()
                    .entity(new GenericEntity<List<Reference>>(objectReferences){})
                    .build();
        } catch (Exception e) {
            LOG.error("Error getting references to {}", query, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error getting references to " + Reference.createFor(query.getDomainObject())))
                    .build();
        } finally {
            LOG.trace("Finish getContainerReferences({})", query);
        }
    }

    @Operation(summary = "Removes a Domain Object",
            description = "uses the References parameter of the DomainQuery"
    )
    @ApiResponses(value = {
            @ApiResponse( responseCode = "200", description = "Successfully removed all domain objects"),
            @ApiResponse( responseCode = "500", description = "Internal Server Error removing domain objects" )
    })
    @POST
    @Path("/domainobject/remove")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeDomainObject(@Parameter DomainQuery query) {
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

    @Operation(summary = "Gets a list of DomainObjects that are referring to this DomainObject",
            description = "Uses reference attribute and reference class to determine type of parent reference to find"
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Successfully got a list of DomainObjects"),
            @ApiResponse(
                    responseCode = "500",
                    description = "Internal Server Error getting list of DomainObjects" )
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/domainobject/reverseLookup")
    public Response getObjectsByReverseRef(@Parameter @QueryParam("subjectKey") final String subjectKey,
                                           @Parameter @QueryParam("referenceId") final Long referenceId,
                                           @Parameter @QueryParam("count") final Long count,
                                           @Parameter @QueryParam("referenceAttr") final String referenceAttr,
                                           @Parameter @QueryParam("referenceClass") final String referenceClass) {
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

    @Operation(summary = "Retrieve all search attributes")
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Successfully got search attributes mapping"),
            @ApiResponse(
                    responseCode = "500",
                    description = "Internal Server Error getting search attributes")
    })
    @GET
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/object_attribute_label_mapping")
    public Response getAllSearchAttributes() {
        LOG.trace("Start getAllSearchAttributes()");
        try {
            Map<String, List<String>> searchAttributes = DomainUtils.getAllSearchAttributes();
            return Response.ok()
                    .entity(new GenericEntity<Map<String, List<String>>>(searchAttributes){})
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred retrieving all search attributes", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error occurred retrieving all search attributes"))
                    .build();
        } finally {
            LOG.trace("Finish getAllSearchAttributes()");
        }
    }
}

package org.janelia.jacs2.rest.sync.v2.dataresources.search;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.collections4.CollectionUtils;
import org.glassfish.jersey.server.ManagedAsync;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.dataservice.search.DocumentIndexingService;
import org.janelia.jacs2.dataservice.search.IndexBuilderService;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.security.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(value = "Janelia Workstation Domain Data")
@Path("/data")
public class RefreshDomainIndexResource {
    private static final Logger LOG = LoggerFactory.getLogger(RefreshDomainIndexResource.class);

    @RequestScoped
    @Inject
    private IndexBuilderService indexBuilderService;

    @Inject @AsyncIndex
    private ExecutorService asyncTaskExecutor;

    @RequireAuthentication
    @ApiOperation(value = "Refresh the entire search index. " +
            "The operation requires admin privileges because it may require a lot of resources to perform the action " +
            "and I don't want to let anybody to just go and refresh the index")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully performed SOLR index update"),
            @ApiResponse(code = 403, message = "The authorized subject is not an admin"),
            @ApiResponse(code = 500, message = "Internal Server Error performing SOLR index clear")
    })
    @ManagedAsync
    @PUT
    @Path("searchIndex")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    public void refreshDocumentsIndex(@QueryParam("clearIndex") Boolean clearIndex,
                                      @QueryParam("indexedClasses") List<String> indexedClassnamesParam,
                                      @Context ContainerRequestContext containerRequestContext,
                                      @Suspended AsyncResponse asyncResponse) {
        asyncTaskExecutor.submit(() -> {
            LOG.trace("Start updateAllDocumentsIndex()");
            try {
                Subject authorizedSubject = JacsSecurityContextHelper.getAuthorizedSubject(containerRequestContext, Subject.class);
                if (authorizedSubject == null) {
                    LOG.warn("Unauthorized attempt to update the entire search index");
                    asyncResponse.resume(new WebApplicationException(Response.Status.FORBIDDEN));
                    return;
                }
                if (!authorizedSubject.hasWritePrivilege()) {
                    LOG.warn("Non-admin user {} attempted to update the entire search index", authorizedSubject.getName());
                    asyncResponse.resume(new WebApplicationException(Response.Status.FORBIDDEN));
                    return;
                }
                Predicate<Class> indexedClassesFilter;
                if (CollectionUtils.isEmpty(indexedClassnamesParam)) {
                    indexedClassesFilter = clazz -> true;
                } else {
                    Set<String> indexedClassnames = indexedClassnamesParam.stream()
                            .flatMap(cn -> Splitter.on(',').omitEmptyStrings().trimResults().splitToList(cn).stream())
                            .collect(Collectors.toSet());
                    indexedClassesFilter = clazz -> indexedClassnames.contains(clazz.getName()) || indexedClassnames.contains(clazz.getSimpleName());
                }
                int nDocs = indexBuilderService.indexAllDocuments(clearIndex != null && clearIndex, indexedClassesFilter);
                asyncResponse.resume(nDocs);
            } catch (Exception e) {
                LOG.error("Error occurred while deleting document index", e);
                asyncResponse.resume(e);
            } finally {
                LOG.trace("Finished updateAllDocumentsIndex()");
            }
        });
    }

    @RequireAuthentication
    @ApiOperation(value = "Clear search index. The operation requires admin privileges")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully performed SOLR index clear"),
            @ApiResponse(code = 403, message = "The authorized subject is not an admin"),
            @ApiResponse(code = 500, message = "Internal Server Error performing SOLR index clear")
    })
    @DELETE
    @Path("searchIndex")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeDocumentsIndex(@Context ContainerRequestContext containerRequestContext) {
        LOG.trace("Start deleteDocumentsIndex()");
        try {
            Subject authorizedSubject = JacsSecurityContextHelper.getAuthorizedSubject(containerRequestContext, Subject.class);
            if (authorizedSubject == null) {
                LOG.warn("Unauthorized attempt to delete the search index");
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            if (!authorizedSubject.hasWritePrivilege()) {
                LOG.warn("Non-admin user {} attempted to remove search index", authorizedSubject.getName());
                return Response.status(Response.Status.FORBIDDEN).build();
            }
            indexBuilderService.removeIndex();
            return Response.noContent().build();
        } catch (Exception e) {
            LOG.error("Error occurred while deleting document index", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOG.trace("Finished deleteDocumentsIndex()");
        }
    }

}

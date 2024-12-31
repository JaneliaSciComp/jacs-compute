package org.janelia.jacs2.rest.sync.v2.dataresources.search;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.google.common.base.Splitter;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.auth.JacsSecurityContextHelper;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.dataservice.search.IndexBuilderService;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.domain.DomainObject;
import org.janelia.model.security.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "RefreshDomainIndex", description = "Janelia Workstation Domain Data")
@Path("/data")
public class RefreshDomainIndexResource {
    private static final Logger LOG = LoggerFactory.getLogger(RefreshDomainIndexResource.class);

    @Inject
    private IndexBuilderService indexBuilderService;

    @Inject @AsyncIndex
    private ExecutorService asyncTaskExecutor;

    @RequireAuthentication
    @Operation(description = "Refresh the entire search index. " +
            "The operation requires admin privileges because it may require a lot of resources to perform the action " +
            "and I don't want to let anybody to just go and refresh the index")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully performed SOLR index update"),
            @ApiResponse(responseCode = "403", description = "The authorized subject is not an admin"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error performing SOLR index clear")
    })
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
                Predicate<Class<?>> indexedClassesFilter;
                if (CollectionUtils.isEmpty(indexedClassnamesParam)) {
                    indexedClassesFilter = clazz -> true;
                } else {
                    Set<String> indexedClassnames = indexedClassnamesParam.stream()
                            .flatMap(cn -> Splitter.on(',').omitEmptyStrings().trimResults().splitToList(cn).stream())
                            .collect(Collectors.toSet());
                    indexedClassesFilter = clazz -> indexedClassnames.contains(clazz.getName()) || indexedClassnames.contains(clazz.getSimpleName());
                }
                Map<Class<? extends DomainObject>, Integer>  indexedDocs = indexBuilderService.indexAllDocuments(clearIndex != null && clearIndex, indexedClassesFilter);
                asyncResponse.resume(indexedDocs);
            } catch (Exception e) {
                LOG.error("Error occurred while deleting document index", e);
                asyncResponse.resume(e);
            } finally {
                LOG.trace("Finished updateAllDocumentsIndex()");
            }
        });
    }

    @RequireAuthentication
    @Operation(summary = "Clear search index. The operation requires admin privileges")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully performed SOLR index clear"),
            @ApiResponse(responseCode = "403", description = "The authorized subject is not an admin"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error performing SOLR index clear")
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

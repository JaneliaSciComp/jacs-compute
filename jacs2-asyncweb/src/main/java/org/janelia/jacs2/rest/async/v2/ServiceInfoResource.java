package org.janelia.jacs2.rest.async.v2;

import com.google.common.base.Splitter;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.JacsServiceDataManager;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.model.domain.enums.SubjectRole;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.jacs2.page.SortCriteria;
import org.janelia.model.jacs2.page.SortDirection;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@RequestScoped
@Produces("application/json")
@Path("/services")
@Api(value = "JACS Service Info")
public class ServiceInfoResource {
    private static final int DEFAULT_PAGE_SIZE = 100;

    @Inject private Logger logger;
    @Inject private JacsServiceDataManager jacsServiceDataManager;
    @Inject private ServiceRegistry serviceRegistry;

    @RequireAuthentication
    @GET
    @Produces("text/plain")
    @Path("/count")
    @ApiOperation(value = "Count services", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response countServices(@QueryParam("service-name") String serviceName,
                                   @QueryParam("service-id") Long serviceId,
                                   @QueryParam("parent-id") Long parentServiceId,
                                   @QueryParam("root-id") Long rootServiceId,
                                   @QueryParam("service-owner") String serviceOwnerKey,
                                   @QueryParam("service-state") String serviceState,
                                   @QueryParam("service-tags") List<String> serviceTags,
                                   @QueryParam("service-from") Date from,
                                   @QueryParam("service-to") Date to,
                                   @QueryParam("page") Integer pageNumber,
                                   @QueryParam("length") Integer pageLength,
                                   @QueryParam("sort-by") String sortCriteria,
                                   @Context UriInfo uriInfo,
                                   @Context SecurityContext securityContext) {
        JacsServiceData pattern = createSearchServicesPattern(serviceName,
                serviceId,
                parentServiceId,
                rootServiceId,
                serviceOwnerKey,
                serviceState,
                serviceTags,
                uriInfo,
                securityContext);
        long count = jacsServiceDataManager.countServices(pattern, new DataInterval<> (from, to));
        return Response
                .status(Response.Status.OK)
                .entity(count)
                .build();
    }

    @RequireAuthentication
    @GET
    @ApiOperation(value = "Search queued services", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response searchServices(@QueryParam("service-name") String serviceName,
                                   @QueryParam("service-id") Long serviceId,
                                   @QueryParam("parent-id") Long parentServiceId,
                                   @QueryParam("root-id") Long rootServiceId,
                                   @QueryParam("service-owner") String serviceOwnerKey,
                                   @QueryParam("service-state") String serviceState,
                                   @QueryParam("service-tags") List<String> serviceTags,
                                   @QueryParam("service-from") Date from,
                                   @QueryParam("service-to") Date to,
                                   @QueryParam("page") Integer pageNumber,
                                   @QueryParam("length") Integer pageLength,
                                   @QueryParam("sort-by") String sortCriteria,
                                   @Context UriInfo uriInfo,
                                   @Context SecurityContext securityContext) {
        JacsServiceData pattern = createSearchServicesPattern(serviceName,
                serviceId,
                parentServiceId,
                rootServiceId,
                serviceOwnerKey,
                serviceState,
                serviceTags,
                uriInfo,
                securityContext);
        PageResult<JacsServiceData> results = jacsServiceDataManager.searchServices(pattern, new DataInterval<> (from, to), createPageRequest(pageNumber, pageLength, sortCriteria));
        return Response
                .status(Response.Status.OK)
                .entity(results)
                .build();
    }

    private PageRequest createPageRequest(Integer pageNumber, Integer pageLength, String sortCriteria) {
        PageRequest pageRequest = new PageRequest();
        if (pageNumber != null) {
            pageRequest.setPageNumber(pageNumber);
        }
        if (pageLength != null) {
            pageRequest.setPageSize(pageLength);
        } else {
            pageRequest.setPageSize(DEFAULT_PAGE_SIZE);
        }
        if (StringUtils.isNotBlank(sortCriteria)) {
            pageRequest.setSortCriteria(Splitter.on(',')
                    .omitEmptyStrings()
                    .trimResults()
                    .splitToList(sortCriteria).stream()
                    .filter(StringUtils::isNotBlank)
                    .map(this::createSortCriteria)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList())
            );
        }
        return pageRequest;
    }

    private SortCriteria createSortCriteria(String sortDescriptor) {
        List<String> scItemAttrs = Splitter.on(' ').trimResults().splitToList(sortDescriptor);
        if (CollectionUtils.isEmpty(scItemAttrs)) {
            return null;
        }
        String fieldName = scItemAttrs.get(0);
        if (StringUtils.isBlank(fieldName)) {
            return null;
        }
        String sortDirection = null;
        if (scItemAttrs.size() > 1) {
            sortDirection = scItemAttrs.get(1);
        }
        if (StringUtils.equalsIgnoreCase("DESC", sortDirection)) {
            return new SortCriteria(fieldName, SortDirection.DESC);
        } else {
            return new SortCriteria(fieldName, SortDirection.ASC);
        }
    }


    private JacsServiceData createSearchServicesPattern(String serviceName,
                                                        Long serviceId,
                                                        Long parentServiceId,
                                                        Long rootServiceId,
                                                        String serviceOwnerKey,
                                                        String serviceState,
                                                        List<String> serviceTags,
                                                        UriInfo uriInfo,
                                                        SecurityContext securityContext) {
        JacsServiceData pattern = new JacsServiceData();
        pattern.setId(serviceId);
        pattern.setParentServiceId(parentServiceId);
        pattern.setRootServiceId(rootServiceId);
        pattern.setName(serviceName);
        if (securityContext.isUserInRole(SubjectRole.Admin.getRole())) {
            // if user is an admin than the owner can be the one passed in the query
            pattern.setOwnerKey(serviceOwnerKey);
        } else {
            // otherwise only query current user's services
            pattern.setOwnerKey(securityContext.getUserPrincipal().getName());
        }
        pattern.setTags(serviceTags);
        try {
            if (StringUtils.isNotBlank(serviceState)) {
                pattern.setState(JacsServiceState.valueOf(serviceState));
            } else {
                pattern.setState(null);
            }
        } catch (Exception e) {
            logger.error("Invalid state filter {}", serviceState, e);
        }
        uriInfo.getQueryParameters().entrySet().stream()
                .filter(paramEntry -> paramEntry.getKey().startsWith("serviceArg.") && StringUtils.isNotBlank(paramEntry.getKey().substring("serviceArg.".length())))
                .forEach(paramEntry -> {
                    String serviceArgName =  paramEntry.getKey().substring("serviceArg.".length()).trim();
                    int nParamValues = CollectionUtils.size(paramEntry.getValue());
                    if (nParamValues == 1) {
                        pattern.addServiceArg(serviceArgName, paramEntry.getValue().get(0));
                    } else if (nParamValues > 1) {
                        pattern.addServiceArg(serviceArgName, paramEntry.getValue());
                    }
                });
        return pattern;
    }

    @RequireAuthentication
    @GET
    @Path("/{service-instance-id}")
    @ApiOperation(value = "Get service info", notes = "Returns service about a given service")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response getServiceInfo(@PathParam("service-instance-id") Long instanceId,
                                   @Context SecurityContext securityContext) {
        JacsServiceData serviceData = jacsServiceDataManager.retrieveServiceById(BigInteger.valueOf(instanceId));
        if (serviceData == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else if (securityContext.isUserInRole(SubjectRole.Admin.getRole()) || serviceData.canBeAccessedBy(securityContext.getUserPrincipal().getName())) {
            return Response
                    .status(Response.Status.OK)
                    .entity(serviceData)
                    .build();
        } else {
            return Response
                    .status(Response.Status.UNAUTHORIZED)
                    .build();
        }
    }

    @RequireAuthentication
    @PUT
    @Path("/{service-instance-id}")
    @ApiOperation(value = "Update service info", notes = "Updates the info about the given service")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response updateServiceInfo(@PathParam("service-instance-id") Long instanceId,
                                      JacsServiceData si,
                                      @Context SecurityContext securityContext) {
        JacsServiceData serviceData = jacsServiceDataManager.retrieveServiceById(instanceId);
        if (serviceData == null) {
            logger.warn("No service found for {}", instanceId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
        if (serviceData.canBeModifiedBy(securityContext.getUserPrincipal().getName())) {
            serviceData = jacsServiceDataManager.updateService(instanceId, si);
            return Response
                    .status(Response.Status.OK)
                    .entity(serviceData)
                    .build();
        } else {
            logger.warn("Service {} cannot be modified by {}", serviceData, securityContext.getUserPrincipal().getName());
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .build();
        }
    }

    @GET
    @Path("/metadata")
    @ApiOperation(value = "Get metadata about all services", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response getAllServicesMetadata() {
        List<ServiceMetaData> services = serviceRegistry.getAllServicesMetadata();
        return Response
                .status(Response.Status.OK)
                .entity(services)
                .build();
    }

    @GET
    @Path("/metadata/{service-name}")
    @ApiOperation(value = "Get metadata about a given service", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 404, message = "If the service name is invalid"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response getServiceMetadata(@PathParam("service-name") String serviceName) {
        ServiceMetaData smd = serviceRegistry.getServiceMetadata(serviceName);
        if (smd == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
        return Response
                .status(Response.Status.OK)
                .entity(smd)
                .build();
    }

}

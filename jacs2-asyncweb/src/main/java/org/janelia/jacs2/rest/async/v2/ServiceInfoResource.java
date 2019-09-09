package org.janelia.jacs2.rest.async.v2;

import com.google.common.base.Splitter;
import com.google.common.io.ByteStreams;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.JacsServiceDataManager;
import org.janelia.jacs2.auth.JacsServiceAccessDataUtils;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.data.NamedData;
import org.janelia.model.domain.enums.SubjectRole;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.jacs2.page.SortCriteria;
import org.janelia.model.jacs2.page.SortDirection;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;
import org.slf4j.Logger;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Api(value = "JACS Service Info")
@RequestScoped
@Produces("application/json")
@Path("/services")
public class ServiceInfoResource {
    private static final int DEFAULT_PAGE_SIZE = 100;

    @Inject private Logger logger;
    @Inject private JacsServiceDataManager jacsServiceDataManager;

    @ApiOperation(value = "Count services", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    @RequireAuthentication
    @GET
    @Produces("text/plain")
    @Path("/count")
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

    @ApiOperation(value = "Search queued services", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    @RequireAuthentication
    @GET
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

    @ApiOperation(value = "Get service info", notes = "Returns data about a given service")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    @RequireAuthentication
    @GET
    @Path("/{service-instance-id}")
    public Response getServiceInfo(@PathParam("service-instance-id") Long instanceId,
                                   @Context SecurityContext securityContext) {
        JacsServiceData serviceData = jacsServiceDataManager.retrieveServiceById(BigInteger.valueOf(instanceId));
        if (serviceData == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else if (JacsServiceAccessDataUtils.canServiceBeAccessedBy(serviceData, securityContext)) {
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

    @ApiOperation(value = "Get service info", notes = "Returns service standard output")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    @RequireAuthentication
    @GET
    @Produces({"application/json", "application/octet-stream"})
    @Path("/{service-instance-id}/job-output")
    public Response getServiceStandardOutput(@PathParam("service-instance-id") Long instanceId,
                                             @Context SecurityContext securityContext) {
        JacsServiceData serviceData = jacsServiceDataManager.retrieveServiceById(BigInteger.valueOf(instanceId));
        if (serviceData == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else if (JacsServiceAccessDataUtils.canServiceBeAccessedBy(serviceData, securityContext)) {
            long fileSize = jacsServiceDataManager.getServiceStdOutputSize(serviceData);
            StreamingOutput fileStream = output -> {
                try {
                    jacsServiceDataManager.streamServiceStdOutput(serviceData)
                            .forEach(isProvider -> {
                                NamedData<InputStream> namedStream;
                                try {
                                    namedStream = isProvider.get();
                                    try {
                                        output.write((namedStream.getName() + "\n").getBytes());
                                        ByteStreams.copy(namedStream.getData(), output);
                                    } catch (IOException ioex) {
                                        logger.error("Error while streaming service {} standard output", serviceData, ioex);
                                    } finally {
                                        try {
                                            namedStream.getData().close();
                                        } catch (IOException ignore) {
                                        }
                                    }
                                } catch (Exception ex) {
                                    logger.error("Error while opening the output stream(s) for {}", serviceData, ex);
                                }
                            });
                } catch (Exception e) {
                    logger.error("Error streaming job output content from {} for {}", serviceData.getOutputPath(), serviceData, e);
                    throw new WebApplicationException(e);
                }
            };
            return Response
                    .ok(fileStream, MediaType.APPLICATION_OCTET_STREAM)
                    .header("Content-Length", fileSize)
                    .header("Content-Disposition", "attachment; filename = " + serviceData.getName() + "-" + serviceData.getId() + "-stdout")
                    .build()
                    ;
        } else {
            return Response
                    .status(Response.Status.UNAUTHORIZED)
                    .build();
        }
    }

    @ApiOperation(value = "Get service info", notes = "Returns service standard error")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    @RequireAuthentication
    @GET
    @Produces({"application/json", "application/octet-stream"})
    @Path("/{service-instance-id}/job-errors")
    public Response getServiceStandardError(@PathParam("service-instance-id") Long instanceId,
                                             @Context SecurityContext securityContext) {
        JacsServiceData serviceData = jacsServiceDataManager.retrieveServiceById(BigInteger.valueOf(instanceId));
        if (serviceData == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else if (JacsServiceAccessDataUtils.canServiceBeAccessedBy(serviceData, securityContext)) {
            long fileSize = jacsServiceDataManager.getServiceStdErrorSize(serviceData);
            StreamingOutput fileStream = output -> {
                try {
                    jacsServiceDataManager.streamServiceStdError(serviceData)
                            .forEach(isProvider -> {
                                NamedData<InputStream> namedStream;
                                try {
                                    namedStream = isProvider.get();
                                    try {
                                        output.write((namedStream.getName() + "\n").getBytes());
                                        ByteStreams.copy(namedStream.getData(), output);
                                    } catch (IOException ioex) {
                                        logger.error("Error while streaming service {} standard error", serviceData, ioex);
                                    } finally {
                                        try {
                                            namedStream.getData().close();
                                        } catch (IOException ignore) {
                                        }
                                    }
                                } catch (Exception ex) {
                                    logger.error("Error while opening the error stream(s) for {}", serviceData, ex);
                                }
                            });
                } catch (Exception e) {
                    logger.error("Error streaming job error content from {} for {}", serviceData.getErrorPath(), serviceData, e);
                    throw new WebApplicationException(e);
                }
            };
            return Response
                    .ok(fileStream, MediaType.APPLICATION_OCTET_STREAM)
                    .header("Content-Length", fileSize)
                    .header("Content-Disposition", "attachment; filename = " + serviceData.getName() + "-" + serviceData.getId() + "-stderr")
                    .build()
                    ;
        } else {
            return Response
                    .status(Response.Status.UNAUTHORIZED)
                    .build();
        }
    }

}

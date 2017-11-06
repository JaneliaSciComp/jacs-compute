package org.janelia.jacs2.rest.async.v2;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ServiceMetaData;
import org.janelia.jacs2.asyncservice.JacsServiceDataManager;
import org.janelia.jacs2.asyncservice.ServiceRegistry;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;

@ApplicationScoped
@Produces("application/json")
@Path("/services")
@Api(value = "JACS Service Info")
public class ServiceInfoResource {
    private static final int DEFAULT_PAGE_SIZE = 100;

    @Inject private JacsServiceDataManager jacsServiceDataManager;
    @Inject private ServiceRegistry serviceRegistry;
    @Inject private Logger logger;

    @GET
    @ApiOperation(value = "Get all services", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response getAllServices(@QueryParam("service-name") String serviceName,
                                   @QueryParam("service-id") Long serviceId,
                                   @QueryParam("parent-id") Long parentServiceId,
                                   @QueryParam("root-id") Long rootServiceId,
                                   @QueryParam("service-owner") String serviceOwner,
                                   @QueryParam("service-state") String serviceState,
                                   @QueryParam("service-from") Date from,
                                   @QueryParam("service-to") Date to,
                                   @QueryParam("page") Integer pageNumber,
                                   @QueryParam("length") Integer pageLength) {
        JacsServiceData pattern = new JacsServiceData();
        pattern.setId(serviceId);
        pattern.setParentServiceId(parentServiceId);
        pattern.setRootServiceId(rootServiceId);
        pattern.setName(serviceName);
        pattern.setOwner(serviceOwner);
        try {
            if (StringUtils.isNotBlank(serviceState)) {
                pattern.setState(JacsServiceState.valueOf(serviceState));
            } else {
                pattern.setState(null);
            }
        } catch (Exception e) {
            logger.error("Invalid state filter {}", serviceState, e);
        }
        PageRequest pageRequest = new PageRequest();
        if (pageNumber != null) {
            pageRequest.setPageNumber(pageNumber);
        }
        if (pageLength != null) {
            pageRequest.setPageSize(pageLength);
        } else {
            pageRequest.setPageSize(DEFAULT_PAGE_SIZE);
        }
        PageResult<JacsServiceData> results = jacsServiceDataManager.searchServices(pattern, new DataInterval<>(from, to), pageRequest);
        return Response
                .status(Response.Status.OK)
                .entity(results)
                .build();
    }

    @GET
    @Path("/{service-instance-id}")
    @ApiOperation(value = "Get service info", notes = "Returns service about a given service")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response getServiceInfo(@PathParam("service-instance-id") Long instanceId) {
        JacsServiceData serviceData = jacsServiceDataManager.retrieveServiceById(BigInteger.valueOf(instanceId));
        if (serviceData == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else {
            return Response
                    .status(Response.Status.OK)
                    .entity(serviceData)
                    .build();
        }
    }

    @PUT
    @Path("/{service-instance-id}")
    @ApiOperation(value = "Update service info", notes = "Updates the info about the given service")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success"),
            @ApiResponse(code = 500, message = "Error occurred") })
    public Response updateServiceInfo(@PathParam("service-instance-id") Long instanceId, JacsServiceData si) {
        JacsServiceData serviceData = jacsServiceDataManager.updateService(instanceId, si);
        if (serviceData == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else {
            return Response
                    .status(Response.Status.OK)
                    .entity(serviceData)
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

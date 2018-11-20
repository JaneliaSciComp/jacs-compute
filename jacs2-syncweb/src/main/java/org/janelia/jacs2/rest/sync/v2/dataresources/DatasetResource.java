package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.DatasetDao;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.sample.DataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriBuilder;
import java.math.BigDecimal;
import java.util.List;

@Api(value = "Janelia Workstation Domain Data")
@Path("/data")
public class DatasetResource {
    private static final Logger LOG = LoggerFactory.getLogger(DatasetResource.class);
    private static final BigDecimal TERRA_BYTES = new BigDecimal(1024).pow(4);

    @Inject
    private DatasetDao datasetDao;
    @Inject
    private LegacyDomainDao legacyDomainDao;

    @ApiOperation(value = "Gets a List of DataSets for the User",
            notes = "Uses the subject key to return a list of DataSets for the user"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the list of datasets",  response = DataSet.class,
                    responseContainer = "List" ),
            @ApiResponse( code = 500, message = "Internal Server Error fetching teh datasets" )
    })
    @GET
    @Path("dataset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDatasets(@ApiParam @QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getDataSets({})", subjectKey);
        try {
            List<DataSet> dataSets = datasetDao.findByOwnerKey(subjectKey);
            return Response
                    .ok(new GenericEntity<List<DataSet>>(dataSets){})
                    .build();
        } finally {
            LOG.trace("Finished getDataSets({})", subjectKey);
        }
    }

    @RequireAuthentication
    @ApiOperation(value = "Gets a List of DataSets for the User",
            notes = "Uses the subject key to return a list of DataSets for the user"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the list of datasets",  response = DataSet.class,
                    responseContainer = "List" ),
            @ApiResponse( code = 500, message = "Internal Server Error fetching teh datasets" )
    })
    @GET
    @Path("dataset/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDatasetById(@ApiParam @PathParam("id") Long id,
                                   @Context SecurityContext securityContext) {
        LOG.trace("Start getDatasetById({}) by {}", id, securityContext.getUserPrincipal());
        try {
            DataSet dataset = datasetDao.findByIdAndSubjectKey(id, securityContext.getUserPrincipal().getName());
            if (dataset != null) {
                return Response
                        .ok(dataset)
                        .build();
            } else {
                DataSet existingDataset = datasetDao.findById(id); // this is only for logging purposes
                if (existingDataset == null) {
                    LOG.info("A dataset exists for {} but is not accessible by {}", id, securityContext.getUserPrincipal());
                    return Response.status(Response.Status.FORBIDDEN)
                            .entity(new ErrorResponse("Dataset is not accessible"))
                            .build();
                } else {
                    LOG.info("No dataset found for {}", id);
                    return Response.status(Response.Status.NOT_FOUND)
                            .entity(new ErrorResponse("Dataset does not exist"))
                            .build();
                }
            }
        } finally {
            LOG.trace("Finished getDatasetById({}) by {}", id, securityContext.getUserPrincipal());
        }
    }

    @RequireAuthentication
    @ApiOperation(value = "Creates a DataSet using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully created a DataSet",
                    response = DataSet.class),
            @ApiResponse( code = 500, message = "Internal Server Error creating a dataset" )
    })
    @PUT
    @Path("dataset")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createDataSet(DomainQuery query) {
        LOG.debug("Start createDataSet({})", query);
        try {
            DataSet dataset = legacyDomainDao.createDataSet(query.getSubjectKey(), query.getDomainObjectAs(DataSet.class));
            return Response
                    .created(UriBuilder.fromMethod(this.getClass(), "getDatasetById").build(dataset.getId()))
                    .entity(dataset)
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred creating a DataSet with {}", query, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error while creating a dataset from " + query))
                    .build();
        } finally {
            LOG.debug("Finished createDataSet({})", query);
        }
    }

    @RequireAuthentication
    @ApiOperation(value = "Updates a DataSet using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated a DataSet",
                    response = DataSet.class),
            @ApiResponse(code = 500, message = "Internal Server Error updating a dataset")
    })
    @POST
    @Path("dataset")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateDataSet(DomainQuery query) {
        LOG.debug("Start updateDataSet({})", query);
        try {
            DataSet dataset = datasetDao.saveWithSubjectKey(query.getDomainObjectAs(DataSet.class), query.getSubjectKey());
            return Response
                    .ok(dataset)
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred updating the dataset {}", query, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error while updating the dataset " + query))
                    .build();
        } finally {
            LOG.debug("Finished updateDataSet({})", query);
        }
    }

    @ApiOperation(value = "Removes the DataSet using the DataSet Id")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully removed a DataSet"),
            @ApiResponse( code = 500, message = "Internal Server Error removing a dataset" )
    })
    @DELETE
    @Path("dataset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeDataSet(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                  @ApiParam @QueryParam("dataSetId") final String datasetIdParam) {
        LOG.debug("Start removeDataSet({}, dataSetId={})", subjectKey, datasetIdParam);
        Long datasetId;
        try {
            try {
                datasetId = new Long(datasetIdParam);
            } catch (Exception e) {
                LOG.error("Invalid dataset ID: {}", datasetIdParam, e);
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("Invalid dataset ID"))
                        .build();
            }
            datasetDao.deleteByIdAndSubjectKey(datasetId, subjectKey);
            return Response.noContent()
                    .build();
        } finally {
            LOG.debug("Finished removeDataSet({}, dataSetId={})", subjectKey, datasetIdParam);
        }
    }

}

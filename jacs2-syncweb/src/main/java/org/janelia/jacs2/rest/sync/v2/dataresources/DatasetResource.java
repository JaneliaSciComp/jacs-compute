package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

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
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.DatasetDao;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.sample.DataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class DatasetResource {
    private static final Logger LOG = LoggerFactory.getLogger(DatasetResource.class);

    @AsyncIndex
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
    @Produces(MediaType.APPLICATION_JSON)
    @Path("dataset")
    public Response getDatasets(@ApiParam @QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getDataSets({})", subjectKey);
        try {
            List<DataSet> dataSets = datasetDao.findOwnedEntitiesBySubjectKey(subjectKey, 0, -1);
            return Response
                    .ok(new GenericEntity<List<DataSet>>(dataSets){})
                    .build();
        } finally {
            LOG.trace("Finished getDataSets({})", subjectKey);
        }
    }

    @ApiOperation(value = "Gets a List of DataSets for the User",
            notes = "Uses the subject key to return a list of DataSets for the user"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched the list of datasets",  response = DataSet.class,
                    responseContainer = "List" ),
            @ApiResponse( code = 500, message = "Internal Server Error fetching teh datasets" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("dataset/{id}")
    public Response getDatasetById(@ApiParam @PathParam("id") Long id,
                                   @Context SecurityContext securityContext) {
        LOG.trace("Start getDatasetById({}) by {}", id, securityContext.getUserPrincipal());
        try {
            DataSet dataset = datasetDao.findEntityByIdAccessibleBySubjectKey(id, securityContext.getUserPrincipal().getName());
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

    @ApiOperation(value = "Creates a DataSet using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully created a DataSet",
                    response = DataSet.class),
            @ApiResponse( code = 500, message = "Internal Server Error creating a dataset" )
    })
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("dataset")
    public Response createDataSet(DomainQuery query) {
        LOG.trace("Start createDataSet({})", query);
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
            LOG.trace("Finished createDataSet({})", query);
        }
    }

    @ApiOperation(value = "Updates a DataSet using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated a DataSet",
                    response = DataSet.class),
            @ApiResponse(code = 500, message = "Internal Server Error updating a dataset")
    })
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("dataset")
    public Response updateDataSet(DomainQuery query) {
        LOG.trace("Start updateDataSet({})", query);
        try {
            DataSet dataset = datasetDao.saveBySubjectKey(query.getDomainObjectAs(DataSet.class), query.getSubjectKey());
            return Response
                    .ok(dataset)
                    .build();
        } catch (Exception e) {
            LOG.error("Error occurred updating the dataset {}", query, e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("Error while updating the dataset " + query))
                    .build();
        } finally {
            LOG.trace("Finished updateDataSet({})", query);
        }
    }

    @ApiOperation(value = "Removes the DataSet using the DataSet Id")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully removed a DataSet"),
            @ApiResponse( code = 500, message = "Internal Server Error removing a dataset" )
    })
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("dataset")
    public Response removeDataSet(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                  @ApiParam @QueryParam("dataSetId") final String datasetIdParam) {
        LOG.trace("Start removeDataSet({}, dataSetId={})", subjectKey, datasetIdParam);
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
            LOG.trace("Finished removeDataSet({}, dataSetId={})", subjectKey, datasetIdParam);
        }
    }

    @ApiOperation(value = "Gets a distinct list of all datasets")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("dataset/all")
    public List<String> getAllDatasetNames() {
        LOG.trace("Start getAllDatasetNames()");
        try {
            return datasetDao.getAllDatasetNames();
        } finally {
            LOG.trace("Finished getAllDatasetNames()");
        }
    }

    @ApiOperation(value = "Gets a list of all data sets available for color depth search in a certain alignment space")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("dataset/colordepth")
    public List<DataSet> getColorDepthDatasets(@ApiParam @QueryParam("subjectKey") final String subjectKey,
                                               @ApiParam @QueryParam("alignmentSpace") final String alignmentSpace) {
        LOG.trace("Start getColorDepthDatasets(subject={}, alignmentSpace={})", subjectKey, alignmentSpace);
        try {
            return legacyDomainDao.getDataSetsWithColorDepthImages(subjectKey, alignmentSpace);
        } finally {
            LOG.trace("Finished getColorDepthDatasets(subject={}, alignmentSpace={})", subjectKey, alignmentSpace);
        }
    }

    @ApiOperation(
            value = "Gets the default pipelines for datasets",
            notes = "Uses the subject key to return a list of DataSets for the user"
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully fetched list of dataset-pipeline matches",  response = Map.class),
            @ApiResponse( code = 500, message = "Internal Server Error list of dataset-pipeline matches" )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("dataset/pipeline")
    public Map<String, String> getDatasetPipelines() {
        LOG.trace("Start getDatasetPipelines()");
        try {
            List<DataSet> dataSets = datasetDao.findEntitiesReadableBySubjectKey(null, 0, -1);
            return dataSets.stream()
                    .filter(ds -> CollectionUtils.isNotEmpty(ds.getPipelineProcesses()))
                    .collect(Collectors.toMap(
                            DataSet::getIdentifier,
                            ds->"PipelineConfig_" + ds.getPipelineProcesses().stream().reduce("", (p1, p2) -> p2)));
        } finally {
            LOG.trace("Finished getDatasetPipelines()");
        }
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    static class SageSyncedDataSet {
        @JsonProperty("dataSetIdentifier")
        String identifier;
        @JsonProperty
        String name;
        @JsonProperty
        String sageSync;
        @JsonProperty
        String user;
    }

    @JacksonXmlRootElement(localName = "dataSetList")
    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    static class SageSyncedDataSets {
        @JacksonXmlElementWrapper(localName = "dataSetList", useWrapping = false)
        @JsonProperty("dataSetList")
        @JacksonXmlProperty(localName = "dataSet")
        final List<SageSyncedDataSet> syncedDataSets;

        SageSyncedDataSets(List<SageSyncedDataSet> syncedDataSets) {
            this.syncedDataSets = syncedDataSets;
        }
    }

    @ApiOperation(value = "Gets Sage synced Data Set")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched list of datasets synced with SAGE",
                    response = SageSyncedDataSet.class,
                    responseContainer = "List"),
            @ApiResponse(code = 500, message = "Internal Server Error list of dataset synced with SAGE")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("dataSet/sage")
    public SageSyncedDataSets getSageSyncedDataSets(@ApiParam @QueryParam("owners") final List<String> owners,
                                                    @ApiParam @QueryParam("sageSync") final Boolean sageSync) {
        LOG.trace("Start getSageSyncDataSets(owners={}, sageSync={})", owners, sageSync);
        try {
            List<DataSet> dataSets = datasetDao.getDatasetsByOwnersAndSageSyncFlag(owners, sageSync);
            List<SageSyncedDataSet> sageSyncedDataSets = dataSets.stream()
                    .map(ds -> {
                        SageSyncedDataSet sds = new SageSyncedDataSet();
                        sds.identifier = ds.getIdentifier();
                        sds.name = ds.getName();
                        if (ds.isSageSync()) {
                            sds.sageSync = "SAGE Sync";
                        }
                        sds.user = ds.getOwnerKey();
                        return sds;
                    })
                    .collect(Collectors.toList());
            return new SageSyncedDataSets(sageSyncedDataSets);
        } finally {
            LOG.trace("Finished getSageSyncDataSets(owners={}, sageSync={})", owners, sageSync);
        }
    }
}

package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.GenericEntity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.UriBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.cdi.AsyncIndex;
import org.janelia.model.access.dao.LegacyDomainDao;
import org.janelia.model.access.domain.dao.DatasetDao;
import org.janelia.model.domain.dto.DomainQuery;
import org.janelia.model.domain.gui.cdmip.ColorDepthLibrary;
import org.janelia.model.domain.sample.DataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "Dataset",
        description = "Janelia Workstation Domain Data"
)
@RequireAuthentication
@ApplicationScoped
@Path("/data")
public class DatasetResource {
    private static final Logger LOG = LoggerFactory.getLogger(DatasetResource.class);

    @AsyncIndex
    @Inject
    private DatasetDao datasetDao;
    @Inject
    private LegacyDomainDao legacyDomainDao;

    @Operation(summary = "Gets a List of DataSets for the User",
            description = "Uses the subject key to return a list of DataSets for the user"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully fetched the list of datasets"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error fetching teh datasets")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("dataset")
    public Response getDatasets(@Parameter @QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getDataSets({})", subjectKey);
        try {
            List<DataSet> dataSets = datasetDao.findEntitiesReadableBySubjectKey(subjectKey, 0, -1);
            return Response
                    .ok(new GenericEntity<List<DataSet>>(dataSets) {
                    })
                    .build();
        } finally {
            LOG.trace("Finished getDataSets({})", subjectKey);
        }
    }

    @Operation(summary = "Gets a List of DataSets for the User",
            description = "Uses the subject key to return a list of DataSets for the user"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully fetched the list of datasets"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error fetching teh datasets")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("dataset/{id}")
    public Response getDatasetById(@Parameter @PathParam("id") Long id,
                                   @Context SecurityContext securityContext) {
        LOG.trace("Start getDatasetById({}) by {}", id, securityContext.getUserPrincipal());
        try {
            DataSet dataset = datasetDao.findEntityByIdReadableBySubjectKey(id, securityContext.getUserPrincipal().getName());
            if (dataset != null) {
                return Response
                        .ok(dataset)
                        .build();
            } else {
                DataSet existingDataset = datasetDao.findById(id); // this is only for logging purposes
                if (existingDataset != null) {
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

    @Operation(summary = "Creates a DataSet using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully created a DataSet"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error creating a dataset")
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

    @Operation(summary = "Updates a DataSet using the DomainObject parameter of the DomainQuery")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully updated a DataSet"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error updating a dataset")
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
                    .contentLocation(UriBuilder.fromMethod(DatasetResource.class, "getDatasetById").build(dataset.getId()))
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

    @Operation(summary = "Removes the DataSet using the DataSet Id")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully removed a DataSet"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error removing a dataset")
    })
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("dataset")
    public Response removeDataSet(@Parameter @QueryParam("subjectKey") final String subjectKey,
                                  @Parameter @QueryParam("dataSetId") final String datasetIdParam) {
        LOG.trace("Start removeDataSet({}, dataSetId={})", subjectKey, datasetIdParam);
        Long datasetId;
        try {
            try {
                datasetId = Long.valueOf(datasetIdParam);
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

    @Operation(summary = "Gets a distinct list of all datasets")
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

    @Operation(summary = "Gets a list of all data sets available for color depth search in a certain alignment space")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("dataset/colordepth")
    public List<ColorDepthLibrary> getLibrariesWithColorDepthImages(@Parameter @QueryParam("subjectKey") final String subjectKey,
                                                                    @Parameter @QueryParam("alignmentSpace") final String alignmentSpace) {
        LOG.trace("Start getLibrariesWithColorDepthImages(subject={}, alignmentSpace={})", subjectKey, alignmentSpace);
        try {
            return legacyDomainDao.getLibrariesWithColorDepthImages(subjectKey, alignmentSpace);
        } finally {
            LOG.trace("Finished getColorDepthDatasets(subject={}, alignmentSpace={})", subjectKey, alignmentSpace);
        }
    }

    @Operation(
            summary = "Gets the default pipelines for datasets",
            description = "Uses the subject key to return a list of DataSets for the user"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully fetched list of dataset-pipeline matches"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error list of dataset-pipeline matches")
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
                            ds -> "PipelineConfig_" + ds.getPipelineProcesses().stream().reduce("", (p1, p2) -> p2)));
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

    @Operation(summary = "Gets Sage synced Data Set")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully fetched list of datasets synced with SAGE"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error list of dataset synced with SAGE")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("dataSet/sage")
    public SageSyncedDataSets getSageSyncedDataSets(@Parameter @QueryParam("owners") final List<String> owners,
                                                    @Parameter @QueryParam("sageSync") final Boolean sageSync) {
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

package org.janelia.jacs2.rest.sync.v2.dataresources;

import java.math.BigDecimal;
import java.math.RoundingMode;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.auth.annotations.RequireAuthentication;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.jacsstorage.clients.api.JadeStorageAttributes;
import org.janelia.model.access.domain.dao.SummaryDao;
import org.janelia.model.domain.report.DatabaseSummary;
import org.janelia.model.domain.report.DiskUsageSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "DataSummary", description = "Janelia Workstation Domain Data")
@RequireAuthentication
@Path("/data")
public class DataSummaryResource {
    private static final Logger LOG = LoggerFactory.getLogger(DataSummaryResource.class);
    private static final BigDecimal TERRA_BYTES = new BigDecimal(1024).pow(4);

    @Inject
    private SummaryDao summaryDao;
    @Inject
    private StorageService storageService;
    @Inject
    @PropertyValue(name = "Dataset.Storage.DefaultVolume")
    private String defaultVolume;

    @Operation(summary = "Returns a disk usage summary for a given user")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got disk uage summary"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting disk usage summary")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("summary/disk")
    public Response getDiskUsageSummary(@Parameter @QueryParam("volumeName") String volumeNameParam,
                                        @Parameter @QueryParam("subjectKey") String subjectKey,
                                        @HeaderParam("AccessKey") String accessKey,
                                        @HeaderParam("SecretKey") String secretKey) {
        LOG.trace("Start getDiskUsageSummary({}, {})", volumeNameParam, subjectKey);
        try {
            String volumeName;
            if (StringUtils.isBlank(volumeNameParam)) {
                volumeName = defaultVolume;
            } else {
                volumeName = volumeNameParam;
            }
            if (StringUtils.isBlank(volumeName)) {
                // If there is no volume name, return a blank summary.
                // This is for external installations which do not have Janelia's disk quota system.
                return Response.ok(new DiskUsageSummary()).build();
            }
            return storageService.fetchQuotaForUser(
                    volumeName,
                    subjectKey,
                    new JadeStorageAttributes()
                            .setAttributeValue("AccessKey", accessKey)
                            .setAttributeValue("SecretKey", secretKey))
                    .map(quotaUsage -> {
                DiskUsageSummary summary = new DiskUsageSummary();
                BigDecimal totalSpace = summaryDao.getDiskSpaceUsageByOwnerKey(subjectKey);
                Double tb = totalSpace.divide(TERRA_BYTES, 2, RoundingMode.HALF_UP).doubleValue();
                summary.setUserDataSetsTB(tb);
                summary.setQuotaUsage(quotaUsage);
                return summary;
            })
                    .map(diskUsageSummary -> Response.ok(diskUsageSummary).build())
                    .orElseGet(() -> Response.status(Response.Status.BAD_REQUEST).build())
            ;
        } finally {
            LOG.trace("Finished getDataSummary({})", subjectKey);
        }
    }

    @Operation(summary = "Returns a database summary for a given user")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully got data summary"),
            @ApiResponse(responseCode = "500", description = "Internal Server Error getting data summary")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/summary/database")
    public Response getDatabaseSummary(@Parameter @QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getDatabaseSummary({})", subjectKey);
        try {
            if (StringUtils.isBlank(subjectKey)) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse("Invalid subjectKey"))
                        .build();
            }
            DatabaseSummary summary = summaryDao.getDataSummaryBySubjectKey(subjectKey);
            return Response.ok(summary)
                    .build();
        } finally {
            LOG.trace("Finished getDatabaseSummary({})", subjectKey);
        }
    }
}

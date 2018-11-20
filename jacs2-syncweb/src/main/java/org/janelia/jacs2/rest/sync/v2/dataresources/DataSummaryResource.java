package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.jacs2.rest.ErrorResponse;
import org.janelia.model.access.domain.dao.DatasetDao;
import org.janelia.model.access.domain.dao.SummaryDao;
import org.janelia.model.domain.report.DatabaseSummary;
import org.janelia.model.domain.report.DiskUsageSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.math.BigDecimal;
import java.math.RoundingMode;

@Api(value = "Janelia Workstation Data Summary Service")
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

    @GET
    @Path("summary/disk")
    @ApiOperation(value = "Returns a disk usage summary for a given user")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully got disk uage summary", response= DiskUsageSummary.class),
            @ApiResponse( code = 500, message = "Internal Server Error getting disk usage summary" )
    })
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDiskUsageSummary(@ApiParam @QueryParam("volumeName") String volumeNameParam,
                                        @ApiParam @QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getDataSummary({})", subjectKey);
        try {
            DiskUsageSummary summary = new DiskUsageSummary();
            BigDecimal totalSpace = summaryDao.getDiskSpaceUsageByOwnerKey(subjectKey);
            Double tb = totalSpace.divide(TERRA_BYTES, 2, RoundingMode.HALF_UP).doubleValue();
            summary.setUserDataSetsTB(tb);

            String volumeName;
            if (StringUtils.isBlank(volumeNameParam)) {
                volumeName = defaultVolume;
            } else {
                volumeName = volumeNameParam;
            }
            storageService.fetchQuotaForUser(volumeName, subjectKey)
                    .ifPresent(quotaUsage -> summary.setQuotaUsage(quotaUsage));
            return Response.ok(summary)
                    .build();
        } finally {
            LOG.trace("Finished getDataSummary({})", subjectKey);
        }
    }

    @ApiOperation(value = "Returns a database summary for a given user")
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully got data summary", response=DatabaseSummary.class),
            @ApiResponse( code = 500, message = "Internal Server Error getting data summary" )
    })
    @GET
    @Path("/summary/database")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDatabaseSummary(@ApiParam @QueryParam("subjectKey") String subjectKey) {
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

package org.janelia.jacs2.rest.sync.v2.dataresources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.storage.StorageService;
import org.janelia.model.access.domain.dao.DatasetDao;
import org.janelia.model.domain.report.DiskUsageSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.math.BigDecimal;
import java.math.RoundingMode;

@Api(value = "Janelia Workstation Domain Data")
@Path("/data")
public class DatasetResource {
    private static final Logger LOG = LoggerFactory.getLogger(DatasetResource.class);
    private static final BigDecimal TERRA_BYTES = new BigDecimal(1024).pow(4);

    @Inject
    private DatasetDao datasetDao;
    @Inject
    private StorageService storageService;
    @Inject
    @PropertyValue(name = "Dataset.Storage.DefaultVolume")
    private String defaultVolume;

    @GET
    @Path("summary/disk")
    @ApiOperation(value = "Returns a disk usage summary for a given user",
            notes = ""
    )
    @ApiResponses(value = {
            @ApiResponse( code = 200, message = "Successfully got disk uage summary", response= DiskUsageSummary.class),
            @ApiResponse( code = 500, message = "Internal Server Error getting disk usage summary" )
    })
    @Produces(MediaType.APPLICATION_JSON)
    public DiskUsageSummary getDiskUsageSummary(@ApiParam @QueryParam("volumeName") String volumeNameParam,
                                                @ApiParam @QueryParam("subjectKey") String subjectKey) {
        LOG.trace("Start getDataSummary({})", subjectKey);
        try {
            DiskUsageSummary summary = new DiskUsageSummary();
            BigDecimal totalSpace = datasetDao.getDiskSpaceUsageByOwnerKey(subjectKey);
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
            return summary;
        } finally {
            LOG.trace("Finished getDataSummary({})", subjectKey);
        }
    }

}

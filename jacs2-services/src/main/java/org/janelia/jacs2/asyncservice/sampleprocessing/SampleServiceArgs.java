package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;

class SampleServiceArgs extends ServiceArgs {
    @Parameter(names = "-sampleId", description = "Sample ID", required = true)
    Long sampleId;
    @Parameter(names = "-objective",
            description = "Optional sample objective. If specified it retrieves all sample image files, otherwise it only retrieves the ones for the given objective",
            required = false)
    String sampleObjective;
    @Parameter(names = "-area",
            description = "Optional sample area. If specified it filters images by the specified area",
            required = false)
    String sampleArea;
    @Parameter(names = "-sampleDataRootDir", description = "Sample root data directory", required = false)
    String sampleDataRootDir;
    @Parameter(names = "-sampleLsmsSubDir", description = "Sample LSMs data subdirectory", required = false)
    String sampleLsmsSubDir;
    @Parameter(names = "-sampleSummarySubDir", description = "Sample summary data subdirectory", required = false)
    String sampleSummarySubDir;
    @Parameter(names = "-sampleSitchingSubDir", description = "Sample stitching data subdirectory", required = false)
    String sampleSitchingSubDir;
    @Parameter(names = "-sampleResultsId", description = "Sample results ID which may need to be passed down from the top service", required = false)
    Long sampleResultsId;
}

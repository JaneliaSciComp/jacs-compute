package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;

import java.util.ArrayList;
import java.util.List;

class DeconvolutionArgs extends ServiceArgs {
    @Parameter(names = {"-i", "-tileChannelConfigurationFiles"},
               description = "Path to the input tile configuration files. Each configuration corresponds to one channel.")
    List<String> tileChannelConfigurationFiles = new ArrayList<>();
    @Parameter(names = {"-p", "-psfFiles"}, description = "Path to the files containing point spread functions. Each psf file correspond to one channel and there must be a 1:1 correspondence with input configuration files", required = true)
    List<String> psfFiles = new ArrayList<>();
    @Parameter(names = {"-z", "-psfZStep"}, description = "PSF Z step in microns.")
    Float psfZStep;
    @Parameter(names = {"-n", "-numIterations"}, description = "Number of deconvolution iterations.")
    Integer nIterations = 10;
    @Parameter(names = {"-v", "-backgroundValue"}, description = "Background intensity value which will be subtracted from the data and the PSF (one per input channel). If omitted, the pivot value estimated in the Flatfield Correction step will be used (default).")
    Float backgroundValue;
    @Parameter(names = {"-c", "-coresPerTask"}, description = "Number of CPU cores used by a single decon task.")
    Integer coresPerTask = 8;

    DeconvolutionArgs() {
        super("Image deconvolution processor");
    }

    void validate() {
        if (tileChannelConfigurationFiles.size() != psfFiles.size()) {
            throw new IllegalArgumentException("Tile configuration files and psf files must have the same size - " +
                    "tileConfigurationFiles: " + tileChannelConfigurationFiles.size() + ", psfFile: " + psfFiles.size());
        }
    }
}

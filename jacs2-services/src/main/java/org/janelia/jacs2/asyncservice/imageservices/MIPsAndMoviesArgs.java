package org.janelia.jacs2.asyncservice.imageservices;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.utils.FileUtils;

class MIPsAndMoviesArgs extends ServiceArgs {
    @Parameter(names = "-imgFile", description = "The name of the image file", required = true)
    String imageFile;
    @Parameter(names = "-imgFilePrefix", description = "Image file prefix")
    String imageFilePrefix;
    @Parameter(names = "-secondImgFile", description = "The name of the image file")
    String secondImageFile;
    @Parameter(names = "-secondImgFilePrefix", description = "Second image file prefix")
    String secondImageFilePrefix;
    @Parameter(names = "-mode", description = "Mode")
    String mode = "none";
    @Parameter(names = "-chanSpec", description = "Channel spec", required = true)
    String chanSpec;
    @Parameter(names = "-colorSpec", description = "Color spec")
    String colorSpec;
    @Parameter(names = "-divSpec", description = "Color spec")
    String divSpec;
    @Parameter(names = "-laser", description = "Laser")
    Integer laser;
    @Parameter(names = "-gain", description = "Gain")
    Integer gain;
    @Parameter(names = "-resultsDir", description = "Results directory", required = false)
    String resultsDir;
    @Parameter(names = "-options", description = "Options", required = false)
    String options = "mips:movies:legends:bcomp";

    String getImageFileName(String imageFileName) {
        return StringUtils.defaultIfBlank(imageFileName, "");
    }

    String getImageFilePrefix(String imageFileName, String definedImageFilePrefix) {
        if (StringUtils.isNotBlank(definedImageFilePrefix)) {
            return definedImageFilePrefix;
        } else  if (StringUtils.isBlank(imageFileName)) {
            return "";
        } else {
            return FileUtils.getFileNameOnly(imageFileName);
        }
    }
}

package org.janelia.jacs2.asyncservice.imagesearch;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.janelia.jacs2.asyncservice.utils.FileUtils;

class ColorDepthFileSearchProcessingUtils {

    static List<File> collectResults(ColorDepthSearchArgs args) {
        return FileUtils.lookupFiles(Paths.get(args.cdMatchesDir), 1, "glob:**/*")
                .filter(Files::isRegularFile)
                .map(Path::toFile)
                .collect(Collectors.toList());
    }

}

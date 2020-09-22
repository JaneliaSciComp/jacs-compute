package org.janelia.jacs2.asyncservice.imagesearch;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.utils.FileUtils;

class CDMMetadataUtils {

    static List<Path> variantPaths(Path variantPath, Path mipPath, String alignmentSpace, Set<String> libraries) {
        if (variantPath.getRoot() != null) {
            return Collections.singletonList(variantPath);
        } else {
            if (CollectionUtils.isEmpty(libraries)) {
                return Collections.singletonList(mipPath.getParent().resolve(variantPath));
            } else {
                int alignmentSpaceIndex = findComponent(mipPath.getParent(), alignmentSpace);
                if (alignmentSpaceIndex == -1) {
                    return Collections.singletonList(variantPath);
                } else {
                    Path alignmentSpaceDir = mipPath.getRoot() == null
                            ? mipPath.subpath(0, alignmentSpaceIndex + 1)
                            : mipPath.getRoot().resolve(mipPath.subpath(0, alignmentSpaceIndex + 1));
                    return libraries.stream()
                            .map(lname -> alignmentSpaceDir.resolve(lname).resolve(variantPath))
                            .collect(Collectors.toList());
                }
            }
        }
    }

    private static int findComponent(Path p, String component) {
        for (int n = p.getNameCount(); n > 1; n--) {
            if (p.getName(n - 1).toString().equals(component)) {
                return n - 1;
            }
        }
        return -1;
    }

    static Stream<String> variantCandidatesStream(List<Path> variantsPaths, String mipPathname) {
        String mipFilenameWithoutExtension = RegExUtils.replacePattern(Paths.get(mipPathname).getFileName().toString(), "\\..*$", "");
        return variantsPaths.stream()
                .flatMap(variantPath -> Stream.of(
                        variantPath.resolve(mipFilenameWithoutExtension + ".png"),
                        variantPath.resolve(mipFilenameWithoutExtension + ".tif")
                ))
                .filter(Files::exists)
                .filter(Files::isRegularFile)
                .map(Path::toString);
    }
}

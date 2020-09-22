package org.janelia.jacs2.asyncservice.imagesearch;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RegExUtils;

class CDMMetadataUtils {

    static Set<Path> variantPaths(Path variantPath, Path mipPath, String alignmentSpace, Set<String> libraries) {
        if (variantPath.getRoot() != null) {
            return Collections.singleton(variantPath);
        } else {
            if (CollectionUtils.isEmpty(libraries)) {
                return Collections.singleton(mipPath.getParent().resolve(variantPath));
            } else {
                int alignmentSpaceIndex = findComponent(mipPath.getParent(), alignmentSpace);
                if (alignmentSpaceIndex == -1) {
                    return Collections.singleton(variantPath);
                } else {
                    Stream<String> mipLibraryNameComponent;
                    if (mipPath.getNameCount() > alignmentSpaceIndex) {
                        mipLibraryNameComponent = Stream.of(mipPath.getName(alignmentSpaceIndex + 1).toString());
                    } else {
                        mipLibraryNameComponent = Stream.of();
                    }
                    Path alignmentSpaceDir = mipPath.getRoot() == null
                            ? mipPath.subpath(0, alignmentSpaceIndex + 1)
                            : mipPath.getRoot().resolve(mipPath.subpath(0, alignmentSpaceIndex + 1));
                    return Stream.concat(mipLibraryNameComponent, libraries.stream())
                            .map(lname -> alignmentSpaceDir.resolve(lname).resolve(variantPath))
                            .collect(Collectors.toSet());
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

    static Stream<String> variantCandidatesStream(Set<Path> variantsPaths, String mipPathname) {
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

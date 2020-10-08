package org.janelia.jacs2.asyncservice.imagesearch;

import java.nio.file.Path;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RegExUtils;

class CDMMetadataUtils {

    static Set<String> variantPaths(String variantName,
                                    Path mipPath,
                                    String alignmentSpace,
                                    Set<String> libraries,
                                    Predicate<Path> variantExistChecker) {
        // the algorithm creates the variant path candidate as follows:
        // it looks up the alignment space component in the mip's folder name
        // if this is found then it builds the possible paths from there by
        // appending the library id from libraries and then the variantName
        // if alignment space component is not found or the libraries set is empty then
        // it appends the variant name to the mip's folder
        Stream<Path> variantPathsStream;
        if (CollectionUtils.isEmpty(libraries)) {
            variantPathsStream = Stream.of(mipPath.getParent().resolve(variantName));
        } else {
            int alignmentSpaceIndex = findComponent(mipPath.getParent(), alignmentSpace);
            if (alignmentSpaceIndex == -1) {
                variantPathsStream = Stream.of(mipPath.getParent().resolve(variantName));
            } else {
                Path alignmentSpaceDir = mipPath.getRoot() == null
                        ? mipPath.subpath(0, alignmentSpaceIndex + 1)
                        : mipPath.getRoot().resolve(mipPath.subpath(0, alignmentSpaceIndex + 1));

                Stream<String> allLibraryIdentifiers;
                if (mipPath.getNameCount() > alignmentSpaceIndex) {
                    // extract the library name component and use this as well together with
                    // the other libraries provided
                    allLibraryIdentifiers = Stream.concat(
                            Stream.of(mipPath.getName(alignmentSpaceIndex + 1).toString()),
                            libraries.stream()
                    );
                } else {
                    allLibraryIdentifiers = libraries.stream();
                }
                variantPathsStream = allLibraryIdentifiers
                        .map(lname -> alignmentSpaceDir.resolve(lname).resolve(variantName));
            }
        }
        // once the candidates paths are available it looks up in the corresponding directories
        // for files with the same name as the source mip, trying out several image file extensions like .png or .tif
        String mipFilenameWithoutExtension = RegExUtils.replacePattern(mipPath.getFileName().toString(), "\\..*$", "");
        return variantPathsStream
                .flatMap(variantPath -> Stream.of(
                        variantPath.resolve(mipFilenameWithoutExtension + ".png"),
                        variantPath.resolve(mipFilenameWithoutExtension + ".tif")
                ))
                .filter(variantExistChecker)
                .map(Path::toString)
                .collect(Collectors.toSet());
    }

    private static int findComponent(Path p, String component) {
        for (int n = p.getNameCount(); n > 1; n--) {
            if (p.getName(n - 1).toString().equals(component)) {
                return n - 1;
            }
        }
        return -1;
    }

}

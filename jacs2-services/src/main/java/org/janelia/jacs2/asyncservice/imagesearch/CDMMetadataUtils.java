package org.janelia.jacs2.asyncservice.imagesearch;

import java.nio.file.Path;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableSet;

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
        // we want to search mips that match either the given mip name or the parent mip name
        // typically a segmented mip looks like:
        // <line>-<slide code>-<objective>-<area>-<alignment space>-<sample id>-CH<channel>-<segment#>_CDM.tif
        // and a non-segmented mip looks like:
        // <line>-<slide code>-<objective>-<area>-<alignment space>-<sample id>-CH<channel>_CDM.tif
        // and the names we are looking for are:
        // <line>-<slide code>-<objective>-<area>-<alignment space>-<sample id>-CH<channel>-<segment#>_CDM.tif
        // or
        // <line>-<slide code>-<objective>-<area>-<alignment space>-<sample id>-CH<channel>_<variant>_CDM.tif
        String mipFilenameWithoutExtension = RegExUtils.replacePattern(mipPath.getFileName().toString(), "\\..*$", "");
        Matcher m = Pattern.compile("CH.-(.+_CDM)").matcher(mipFilenameWithoutExtension);
        Set<String> mipFilenames;
        if(m.find()) {
            mipFilenames = ImmutableSet.of(
                    mipFilenameWithoutExtension + ".png",
                    mipFilenameWithoutExtension + ".tif",
                    mipFilenameWithoutExtension.replace(m.group(1), variantName + "_CDM.png"),
                    mipFilenameWithoutExtension.replace(m.group(1), variantName + "_CDM.tif")
            );
        } else {
            mipFilenames = ImmutableSet.of(
                    mipFilenameWithoutExtension + ".png",
                    mipFilenameWithoutExtension + ".tif"
            );
        }
        return variantPathsStream
                .flatMap(variantPath -> mipFilenames.stream().map(variantPath::resolve))
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

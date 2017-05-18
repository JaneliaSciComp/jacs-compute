package org.janelia.jacs2.asyncservice.imageservices.stitching;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StitchingUtils {

    public static List<String> getMaxTileImageGroup(Path tileGroupsFile) {
        try {
            List<String> groupsFileContent = Files.readAllLines(tileGroupsFile);
            List<List<String>> groups = new ArrayList<>();
            List<String> currentGroup = null;

            List<String> maxGroup = new ArrayList<>();
            for (String l : groupsFileContent) {
                if (StringUtils.isBlank(l)) continue;
                if (l.startsWith("# tiled image group")) {
                    if (CollectionUtils.isNotEmpty(currentGroup)) {
                        groups.add(currentGroup);
                        if (CollectionUtils.size(maxGroup) < currentGroup.size()) {
                            maxGroup = currentGroup;
                        }
                    }
                    currentGroup = new ArrayList<>();
                } else {
                    if (currentGroup == null) continue;
                    currentGroup.add(l);
                }
            }
            if (CollectionUtils.isNotEmpty(currentGroup)) {
                groups.add(currentGroup);
                if (CollectionUtils.size(maxGroup) < currentGroup.size()) {
                    maxGroup = currentGroup;
                }
            }
            return maxGroup;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }


    public static StitchedImageInfo readStitchedImageInfo(Path stitchedImageFile) {
        StitchedImageInfo stitchedImageInfo = new StitchedImageInfo();
        Multimap<String, String> sections = readStitchedImageSections(stitchedImageFile);
        stitchedImageInfo.setThumbnailFile(getSingleValue("thumbnail file", sections));
        stitchedImageInfo.setNumberOfTiles(getSingleValueAsInt("tiles", sections));
        stitchedImageInfo.setDimensions(getSingleValue("dimensions (XYZC)", sections));
        stitchedImageInfo.setOrigin(getSingleValue("origin (XYZ)", sections));
        stitchedImageInfo.setResolution(getSingleValue("resolution (XYZ)", sections));
        stitchedImageInfo.setTileCoordinates(getMultiValue("image coordinates look up table", sections));
        stitchedImageInfo.setMstLut(getSingleValue("MST LUT", sections));
        return stitchedImageInfo;
    }

    private static Multimap<String, String> readStitchedImageSections(Path stitchedImageFile) {
        try {
            List<String> stitchedImageContent = Files.readAllLines(stitchedImageFile);
            Multimap<String, String> sections = LinkedHashMultimap.create();
            String currentSection = null;
            for (String l : stitchedImageContent) {
                String trimmedLine = l.trim();
                if (StringUtils.isEmpty(trimmedLine) || trimmedLine.equalsIgnoreCase("null")) continue;

                if (trimmedLine.startsWith("#")) {
                    currentSection = trimmedLine.substring(1).trim();
                    continue;
                }
                if (StringUtils.isBlank(currentSection)) {
                    continue;
                }
                sections.put(currentSection, trimmedLine);
            }
            return sections;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String getSingleValue(String key, Multimap<String, String> sections) {
        if (CollectionUtils.isNotEmpty(sections.get(key))) {
            return sections.get(key).stream().findFirst().orElse(null);
        } else {
            return null;
        }
    }

    private static int getSingleValueAsInt(String key, Multimap<String, String> sections) {
        String stringValue = getSingleValue(key, sections);
        if (StringUtils.isNotEmpty(stringValue)) {
            return Integer.parseInt(stringValue);
        } else {
            return 0;
        }
    }

    private static List<String> getMultiValue(String key, Multimap<String, String> sections) {
        if (CollectionUtils.isNotEmpty(sections.get(key))) {
            return ImmutableList.copyOf(sections.get(key));
        } else {
            return ImmutableList.of();
        }
    }
}

package org.janelia.jacs2.asyncservice.imageservices.tools;

import com.google.common.base.Splitter;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.jacs2.asyncservice.sampleprocessing.zeiss.LSMChannel;
import org.janelia.jacs2.asyncservice.sampleprocessing.zeiss.LSMMetadata;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.IntStream;

public class LSMProcessingTools {

    public static final String REFERENCE_COLOR = "#FFFFFF";
    public static final String RED_COLOR = "#FF0000";
    public static final String GREEN_COLOR = "#00FF00";
    public static final String BLUE_COLOR = "#0000FF";

    private static final char REFERENCE = 'r';
    private static final char SIGNAL = 's';

    /**
     * Given the number of channels and a 1-based reference channel, create a channel specification.
     * For example, with a 4 channel image, with refChannel at 4, the chanSpec would be "sssr".
     * @param numChannels number of channels in the image (length of the resulting chanSpec)
     * @param oneBasedRefChannel which channel is the 1-based reference channel
     * @return
     */
    public static String createChanSpec(int numChannels, int oneBasedRefChannel) {
        StringBuilder chanSpecBuilder = new StringBuilder();
        IntStream.rangeClosed(1, numChannels)
                .mapToObj(oneBasedCh -> {
                    if (oneBasedCh == oneBasedRefChannel) {
                        return REFERENCE;
                    } else {
                        return SIGNAL;
                    }
                })
                .forEach(chanSpecBuilder::append);
        return chanSpecBuilder.toString();
    }

    public static String createChanSpec(List<String> channelDyeNames, Collection<String> referenceDyes) {
        StringBuilder chanSpecBuilder = new StringBuilder();
        channelDyeNames.stream()
                .map(dye -> referenceDyes.contains(dye) ? REFERENCE : SIGNAL)
                .forEach(chanSpecBuilder::append);
        return chanSpecBuilder.toString();
    }

    public static boolean isReferenceChanSpec(String chSpec) {
        return chSpec.startsWith(String.valueOf(REFERENCE));
    }

    /**
     * Convert from a channel specification (e.g. "ssr") to a list of unique channel identifiers (e.g. ["s0","s1","r0"])
     * @param chanSpec a channel specification (e.g. "ssr")
     * @return a list of unique identifiers, each prepended with either "s" for signal or "r" for reference.
     */
    public static List<String> convertChanSpecToList(String chanSpec) {
        int s = 0;
        int r = 0;
        List<String> channelList = new ArrayList<>();
        for(int sourceIndex=0; sourceIndex<chanSpec.length(); sourceIndex++) {
            char imageChanCode = chanSpec.charAt(sourceIndex);
            switch (imageChanCode) {
                case SIGNAL:
                    channelList.add(SIGNAL + "" + s);
                    s++;
                    break;
                case REFERENCE:
                    channelList.add(REFERENCE + "" + r);
                    r++;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown channel code: " + imageChanCode);
            }
        }
        return channelList;
    }

    public static int getOneBasedRefChannelIndex(LSMMetadata lsmMetadata) {
        int i = 1;
        int refIndex = 1;
        for(LSMChannel lsmChannel : lsmMetadata.getChannels()) {
            if ("#FFFFFF".equalsIgnoreCase(lsmChannel.getColor())) {
                refIndex = i;
                break;
            }
            i++;
        }
        return refIndex;
    }

    public static List<String> getLsmChannelColors(LSMMetadata lsmMetadata) {
        List<String> lsmChannelColors = new ArrayList<>();
        for(LSMChannel lsmChannel : lsmMetadata.getChannels()) {
            lsmChannelColors.add(lsmChannel.getColor());
        }
        return lsmChannelColors;
    }

    /**
     * Read LSMMetadata from the given file.
     * @param lsmMetadataFileName the name of the LSM metadata file
     * @return LSMMetadata struct.
     */
    public static LSMMetadata getLSMMetadata(String lsmMetadataFileName) {
        try {
            return LSMMetadata.fromFile(new File(lsmMetadataFileName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Map<String, Integer> getChannelTagPos(List<String> channelTags) {
        Map<String, Integer> tagPos = new LinkedHashMap<>();
        int sourceChanIndex = 0;
        for (String chanSpec : channelTags) {
            if (!tagPos.containsKey(chanSpec)) {
                tagPos.put(chanSpec, sourceChanIndex);
            }
            ++sourceChanIndex;
        }
        return tagPos;
    }

    public static String generateChannelMapping(List<String> sourceChannels, List<String> targetChannels) {
        Map<String, Integer> sourceChanPos = getChannelTagPos(sourceChannels);

        StringJoiner channelMappingBuilder = new StringJoiner(",");
        int targetChanIndex = 0;
        for (String chanSpec : targetChannels) {
            Integer sourceIndex = sourceChanPos.get(chanSpec);
            if (sourceIndex == null) {
                throw new IllegalArgumentException("Invalid target channel - channel " + chanSpec + " not found in the surce spec " + sourceChannels);
            }
            channelMappingBuilder.add(String.valueOf(sourceIndex));
            channelMappingBuilder.add(String.valueOf(targetChanIndex));
            targetChanIndex++;
        }
        return channelMappingBuilder.toString();
    }

    public static String generateOutputChannelReordering(List<String> sourceChannels, List<String> targetChannels) {
        Map<String, Integer> sourceChanPos = getChannelTagPos(sourceChannels);
        StringJoiner channelMappingBuilder = new StringJoiner(",");
        for (String chanSpec : targetChannels) {
            Integer sourceIndex = sourceChanPos.get(chanSpec);
            if (sourceIndex == null) {
                throw new IllegalArgumentException("Invalid target channel - channel " + chanSpec + " not found in the surce spec " + sourceChannels);
            }
            channelMappingBuilder.add(String.valueOf(sourceIndex));
        }
        return channelMappingBuilder.toString();
    }

    public static ChannelComponents extractChannelComponents(List<String> channelTagList) {
        StringJoiner referenceChannelPosBuilder = new StringJoiner(" ");
        StringJoiner referenceChannelNumberBuilder = new StringJoiner(" ");
        StringJoiner signalChannelBuilder = new StringJoiner(" ");
        StringBuilder chanSpecBuilder = new StringBuilder();
        int tagIndex = 0;
        int nReferenceChannels = 0;
        for (String chanTag : channelTagList) {
            if ("reference".equals(chanTag) || chanTag.matches("r\\d+")) {
                referenceChannelPosBuilder.add(String.valueOf(tagIndex));
                referenceChannelNumberBuilder.add(String.valueOf(tagIndex + 1));
                chanSpecBuilder.append(REFERENCE);
                nReferenceChannels++;
                if (nReferenceChannels > 1) {
                    throw new IllegalArgumentException("More than one reference channel detected: " + channelTagList + " " + tagIndex);
                }
            } else {
                signalChannelBuilder.add(String.valueOf(tagIndex));
                chanSpecBuilder.append(SIGNAL);
            }
            tagIndex++;
        }
        ChannelComponents channelComponents = new ChannelComponents();
        channelComponents.channelSpec = chanSpecBuilder.toString();
        channelComponents.referenceChannelsPos = referenceChannelPosBuilder.toString();
        channelComponents.signalChannelsPos = signalChannelBuilder.toString();
        channelComponents.referenceChannelNumbers = referenceChannelNumberBuilder.toString();
        return channelComponents;
    }

    public static Pair<Multimap<String, String>, Map<String, String>> parseChannelDyeSpec(String channelDyeSpec) {
        Iterable<String> channels = Splitter.on(';').trimResults().omitEmptyStrings().split(channelDyeSpec);

        Multimap<String,String> channelTagToDyesMap = LinkedHashMultimap.create();
        Map<String,String> dyeToTagMap = new HashMap<String,String>();
        for(String channel : channels) {
            String[] parts = channel.split("=");
            String channelTag = parts[0];
            Iterable<String> channelDyes = parseChannelComponents(parts[1]);
            for(String dye : channelDyes) {
                channelTagToDyesMap.put(channelTag, dye);
                if (dyeToTagMap.containsKey(dye)) {
                    throw new IllegalArgumentException("Dye "+dye+" is already mapped as "+dyeToTagMap.get(dye));
                }
                dyeToTagMap.put(dye, channelTag);
            }
        }
        return new ImmutablePair<>(channelTagToDyesMap, dyeToTagMap);
    }

    public static List<String> parseChannelComponents(String channelComponents) {
        return StringUtils.isBlank(channelComponents) ? Collections.emptyList() : Splitter.on(',').trimResults().omitEmptyStrings().splitToList(channelComponents);
    }

    public static String reconcileValues(String v1, String v2) {
        if (StringUtils.isBlank(v1)) return v2;
        else if (StringUtils.isBlank(v2)) return v1;
        else if (StringUtils.equals(v1, v2)) return v1;
        else return null;
    }
}

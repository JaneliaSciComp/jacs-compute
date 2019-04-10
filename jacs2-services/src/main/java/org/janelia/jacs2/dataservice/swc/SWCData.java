package org.janelia.jacs2.dataservice.swc;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class holds data from a file using the .swc format, which is
 * used for storing neuron skeletons; see http://research.mssm.edu/cnic/swc.html
 * for the best description I could find
 *
 * this is designed to be a dumb container; other than a minimal awareness
 * of what nodes should look like and how they interrelate, it doesn't know
 * or care anything about what the data means; the header lines could be
 * gibberish, and you could mark "ends" as "fork points" and it wouldn't care
 *
 * to read a file, call the static method, which returns the data instance;
 * the instance gives you a list of header lines and SWC nodes; it's up to
 * you to transform those lists into whatever kind of neuron object you want
 *
 * the writer method writes the file out; it runs its validator first
 *
 *
 * possible improvements:
 * - add write(String filename) (rather than File)
 * - add setter or equiv. for nodes, header lines?  not really needed; it's
 *      intended that construct the lists elsewhere; SWCData is a dumb wrapper
 *
 *
 * djo, 6/14
 *
 */
public class SWCData {

    private static final Logger LOG = LoggerFactory.getLogger(SWCData.class);
    private static final String STD_SWC_EXTENSION = ".swc";
    private static final String NAME_HEADER_PREFIX = "NAME";
    private static final String COLOR_HEADER_PREFIX = "COLOR";

    private String swcName;
    private List<SWCNode> nodeList = new ArrayList<>();
    private List<String> headerList = new ArrayList<>();

    // neuron center is also encoded in the header list, but
    // some routines want it in original form
    private double[] neuronCenter = {0.0, 0.0, 0.0};

    SWCData(String swcName) {
        this.swcName = swcName;
    }

    void addHeader(String header) {
        headerList.add(header);
    }

    void addNode(SWCNode node) {
        nodeList.add(node);
    }

    /**
     * Check the swcFile;
     * @return null if the SWC structure is valid
     */
    List<String> validate() {

        List<String> validationErrors = new ArrayList<>();

        // check that header lines all start with #
        for (String line: getHeaderList()) {
            if (!line.startsWith("#")) {
                validationErrors.add(String.format("header line doesn't start with #: %s", line));
            }
        }

        int nRoots = 0;
        Set<Integer> parentNodeCandidates = Stream.concat(
                Stream.of(-1), // -1 means root node - no parent
                nodeList.stream().map(node -> node.getIndex())
        ).collect(Collectors.toSet());

        int lastIndex = 0;
        for (SWCNode node: getNodeList()) {
            // node indices should increment by one each line
            if (node.getIndex() != lastIndex + 1) {
                validationErrors.add(String.format("index %d out of order", node.getIndex()));
            } else if (!node.isValid()) {
                // is node valid:
                validationErrors.add(String.format("invalid node (index %d)", node.getIndex()));
            } else  if (!parentNodeCandidates.contains(node.getParentIndex())) {
                // each node parent exists (or is root)
                validationErrors.add(String.format("node with invalid parent index %d", node.getParentIndex()));
            }
            if (node.getParentIndex() == -1) {
                nRoots += 1;
            }
            lastIndex = node.getIndex();
        }
        // there must be at least one root
        if (nRoots == 0) {
            validationErrors.add("no root node");
        }
        return validationErrors;
    }

    Iterable<SWCNode> getNodeList() {
        return nodeList;
    }

    Iterable<String> getHeaderList() {
        return headerList;
    }

    double[] getNeuronCenter() {
        return neuronCenter;
    }

    double[] extractOffset() {
        return findHeaderLine("OFFSET")
                .map(offsetHeader -> {
                    try {
                        // expect # OFFSET x y z
                        String [] items = offsetHeader.split("\\s+");
                        return new double[] {
                                Double.parseDouble(items[2]),
                                Double.parseDouble(items[3]),
                                Double.parseDouble(items[4])
                        };
                    } catch (Exception e) {
                        LOG.warn("Failed to parse offset header {}.", offsetHeader, e);
                        throw new IllegalArgumentException("Failed to parse offset header: " + offsetHeader);
                    }
                })
                .orElseGet(() -> new double[] {0, 0, 0});
    }

    String extractName() {
        return findHeaderLine(NAME_HEADER_PREFIX)
                .map(nameHeader -> {
                    int hdrPos = nameHeader.indexOf(NAME_HEADER_PREFIX);
                    return nameHeader.substring(hdrPos + NAME_HEADER_PREFIX.length()).trim();
                })
                .orElseGet(() -> {
                    if (StringUtils.isBlank(swcName)) {
                        String swcFileName = Paths.get(swcName).getFileName().toString();
                        if (swcFileName.endsWith(STD_SWC_EXTENSION)) {
                            return swcFileName.substring(0, swcFileName.length() - STD_SWC_EXTENSION.length());
                        } else {
                            return swcFileName;
                        }
                    } else {
                        return null;
                    }
                });
    }

    float[] extractColors() {
        return findHeaderLine(COLOR_HEADER_PREFIX)
                .map(colorHeader -> {
                    float[] rgb = new float[3];
                    // NOTE: if fewer colors are in header than 3, remainder
                    // are just filled with 0f.
                    int colorHdrOffs = colorHeader.indexOf(COLOR_HEADER_PREFIX);
                    String rgbHeader = colorHeader.substring(colorHdrOffs + COLOR_HEADER_PREFIX.length()).trim();
                    String[] colors = rgbHeader.split("[, ]");
                    for (int i = 0; i < colors.length  &&  i < rgb.length; i++) {
                        try {
                            rgb[i] = Float.parseFloat(colors[i]);
                        } catch (NumberFormatException nfe) {
                            // Ignore what we cannot parse.
                            LOG.warn("Failed to parse color value {} of header {}.", i, colorHeader);
                        }
                    }
                    return rgb;
                })
                .orElse(null);

    }

    private Optional<String> findHeaderLine(String key) {
        for (String line: headerList) {
            String [] items = line.split("\\s+");
            if (items.length >= 2 && items[1].equals(key)) {
                return Optional.of(line);
            }
        }
        return Optional.empty();
    }

}

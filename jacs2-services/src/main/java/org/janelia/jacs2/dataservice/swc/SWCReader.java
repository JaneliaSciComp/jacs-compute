package org.janelia.jacs2.dataservice.swc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

public class SWCReader {

    private static final Logger LOG = LoggerFactory.getLogger(SWCReader.class);

    SWCData readSWCFile(Path swcFile) {
        SWCData swcData = new SWCData(swcFile);
        try {
            Files.readAllLines(swcFile, Charset.defaultCharset())
                    .stream()
                    .map(l -> l.trim())
                    .filter(l -> l.length() > 0)
                    .forEach(l -> {
                        if (l.startsWith("#")) {
                            swcData.addHeader(l);
                        } else {
                            swcData.addNode(parseSWCNodeLine(l));
                        }
                    });
            return swcData;
        } catch (Exception e) {
            LOG.error("Error parsing SWC file {}", swcFile, e);
            throw new IllegalArgumentException("Error reading " + swcFile, e);
        }
    }

    /**
     * create a node from a line of a swc file; null if it fails
     */
    private static SWCNode parseSWCNodeLine(String line) {
        if (line == null) {
            return null;
        }

        String[] items = line.split("\\s+");
        if (items.length != 7) {
            return null;
        }

        return new SWCNode(
                Integer.parseInt(items[0]),
                SWCNode.SegmentType.fromNumValue(Integer.parseInt(items[1])),
                Double.parseDouble(items[2]),
                Double.parseDouble(items[3]),
                Double.parseDouble(items[4]),
                Double.parseDouble(items[5]),
                Integer.parseInt(items[6])
        );
    }


}

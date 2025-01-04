package org.janelia.jacs2.dataservice.swc;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import jakarta.enterprise.context.Dependent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Dependent
public class SWCReader {

    private static final Logger LOG = LoggerFactory.getLogger(SWCReader.class);

    SWCData readSWCStream(String name, InputStream swcStream) {
        SWCData swcData = new SWCData(name);
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(swcStream, Charset.defaultCharset()));
            reader.lines()
                    .map(l -> l.trim())
                    .filter(l -> l.length() > 0)
                    .forEach(l -> {
                        if (l.startsWith("#")) {
                            swcData.addHeader(l);
                        } else {
                            SWCNode swcNode = parseSWCNodeLine(l);
                            if (swcNode != null) swcData.addNode(swcNode);
                        }
                    });
            return swcData;
        } catch (Exception e) {
            LOG.error("Error parsing SWC stream {}", name, e);
            throw new IllegalArgumentException("Error reading swc stream " + name, e);
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
            LOG.warn("Wrong # of items - expected 7 but found {} on line: {}", items.length, line);
            return null;
        }

        try {
            return new SWCNode(
                    Integer.parseInt(items[0]),
                    Integer.parseInt(items[1]),
                    Double.parseDouble(items[2]),
                    Double.parseDouble(items[3]),
                    Double.parseDouble(items[4]),
                    Double.parseDouble(items[5]),
                    Integer.parseInt(items[6])
            );
        } catch (Exception e) {
            LOG.error("Error encountered while processing node line {}", line, e);
            throw new IllegalArgumentException("Error processing node " + line, e);
        }
    }


}

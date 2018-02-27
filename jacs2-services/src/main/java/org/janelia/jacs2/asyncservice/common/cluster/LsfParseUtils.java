package org.janelia.jacs2.asyncservice.common.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for parsing things returned by LSF.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class LsfParseUtils {

    private static final Logger log = LoggerFactory.getLogger(LsfParseUtils.class);

    private static final int KB = 1024;
    private static final int MB = KB * 1024;
    private static final int GB = MB * 1024;
    private static final int TB = GB * 1024;

    /**
     * Convert the given LocalDateTime instance to an old-school Date.
     * @param ldt
     * @return date in the system time zone
     */
    public static Date convertLocalDateTime(LocalDateTime ldt) {
        if (ldt == null) return null;
        return Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
    }

    /**
     * Calculate the difference in seconds between two local date times.
     * @param startTime
     * @param finishTime
     * @return number of seconds
     */
    public static Long getDiffSecs(LocalDateTime startTime, LocalDateTime finishTime) {
        if (startTime==null || finishTime==null) return null;
        return ChronoUnit.SECONDS.between(startTime, finishTime);
    }

    /**
     * Parse LSF's max_mem field into a normalized number of bytes.
     * @param memLsf
     * @return number of bytes
     */
    public static Long parseMemToBytes(String memLsf) {
        if (memLsf == null) return null;
        Double bytes = null;

        Pattern p = Pattern.compile("([\\d.]+)\\s+(\\w+)");
        Matcher m = p.matcher(memLsf.trim());
        if (m.matches()) {
            double amount = Double.parseDouble(m.group(1));
            String units = m.group(2).toLowerCase();

            if (units.startsWith("k")) {
                bytes = KB * amount;
            }
            else if (units.startsWith("m")) {
                bytes = MB * amount;
            }
            else if (units.startsWith("g")) {
                bytes = GB * amount;
            }
            else if (units.startsWith("t")) {
                // Future proof!
                bytes = TB * amount;
            }
            else {
                log.warn("Could not parse units in max mem: '{}'", units);
                return null;
            }
        }
        else {
            log.warn("Could not parse max mem: '{}'", memLsf);
            return null;
        }

        return bytes.longValue();
    }

    public static void main(String[] args) {
        System.out.println(parseMemToBytes("1.7 Gbytes "));
        System.out.println(parseMemToBytes("1.7 Gbytes"));
        System.out.println(parseMemToBytes(" 1.7   Gbytes  "));
        System.out.println(parseMemToBytes("1.9 Gbytes"));
    }
}

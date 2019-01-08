package org.janelia.jacs2.rest.sync.v2.dataresources.search;

import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Utilities used by the server, which are likewise useful in the client.
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class SolrUtils {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T00:00:00Z'");
    private static final SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    /**
     * Format the given date according to the year/month/day part of the "Complete ISO 8601 Date" format supported by SOLR query syntax.
     *
     * @param date
     * @return
     */
    public static String formatDate(Date date) {
        return DATE_FORMAT.format(date);
    }

    /**
     * Format the given date according to "Complete ISO 8601 Date" format supported by SOLR query syntax.
     *
     * @param date
     * @return
     */
    public static String formatDateTime(Date date) {
        return DATE_TIME_FORMAT.format(date);
    }
    
    /**
     * Format the given name in lowercase, with underscores instead of spaces. For example, "Tiling Pattern" -> "tiling_pattern"
     *
     * @param name
     * @return
     */
    public static String getFormattedName(String name) {
        return name.toLowerCase().replaceAll("\\W+", "_");
    }

    /**
     * Get the SOLR field name from an attribute name. For example, "Tiling Pattern" -> "tiling_pattern_txt"
     *
     * @param name
     * @return
     */
    public static String getDynamicFieldName(String name) {
        return getFormattedName(name) + "_txt";
    }

    /**
     * Get the SOLR field name from an attribute name. For example, "Tiling Pattern" -> "tiling_pattern_txt"
     *
     * @param solrFieldName
     * @return
     */
    public static String getAttributeNameFromSolrFieldName(String solrFieldName) {
        if (solrFieldName == null) {
            return null;
        }
        String name = solrFieldName.replaceFirst("_txt", "");
        return underscoreToTitleCase(name);
    }

    /**
     * Converts the given string from underscore format (e.g. "my_test") to tile case format (e.g. "My Test"
     * @param s
     * @return
     */
    private static String underscoreToTitleCase(String s) {
        if (StringUtils.isBlank(s)) return s;
        String[] words = s.split("_");
        StringBuffer buf = new StringBuffer();
        for(String word : words) {
            if (word.isEmpty()) continue;
            char c = Character.toUpperCase(word.charAt(0));
            if (buf.length()>0) buf.append(' ');
            buf.append(c);
            buf.append(word.substring(1).toLowerCase());
        }
        return buf.toString();
    }
}

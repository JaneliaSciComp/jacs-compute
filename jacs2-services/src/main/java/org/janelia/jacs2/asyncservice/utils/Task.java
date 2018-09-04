package org.janelia.jacs2.asyncservice.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Copied from JACSv1 to enable migration.
 * TODO: move this functionality elsewhere
 */
public class Task {

    public static List<String> listOfStringsFromCsvString(String listString) {
        if (null==listString||"".equals(listString)) {
            return new ArrayList<String>();
        }
        String[] listArr = listString.split(",");
        ArrayList<String> list = new ArrayList<String>();
        for (String aListArr : listArr) {
            list.add(aListArr.trim());
        }
        return list;
    }

    public static String csvStringFromCollection(Collection<?> collection) {
        StringBuilder sb = new StringBuilder("");
        if (collection != null) {
            Iterator iterator = collection.iterator();
            while (iterator.hasNext()) {
                Object next = iterator.next();
                sb.append(next);
                if (iterator.hasNext()) { sb.append(","); }
            }
        }
        return sb.toString();
    }
}

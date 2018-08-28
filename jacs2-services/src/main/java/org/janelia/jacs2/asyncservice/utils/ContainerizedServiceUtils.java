package org.janelia.jacs2.asyncservice.utils;

import org.janelia.model.domain.compute.ContainerizedService;

import java.util.List;

/**
 * Utility methods for dealing with containerized services.
 *
 * TODO: move to this jacs-model
 *
 * @author <a href="mailto:rokickik@janelia.hhmi.org">Konrad Rokicki</a>
 */
public class ContainerizedServiceUtils {

    /**
     * Return the container in the list which matches the given name and version.
     * @param containers list of containers to search
     * @param name target container name
     * @param version target container version
     * @return matching container or null if not match found
     */
    public static ContainerizedService getContainer(List<ContainerizedService> containers, String name, String version) {
        for (ContainerizedService container : containers) {
            if (container.getName().equals(name) && container.getVersion().equals(version)) {
                return container;
            }
        }
        return null;
    }

    /**
     * Sort the given containers by version, from lowest to highest. Ordering rule examples:
     * 1.0-BETA (anything with a dash comes before the real version)
     * 1.0
     * 1.0.2 (more specific versions are assumed to come later)
     * 2.0.1
     * 3
     *
     * @param containers
     */
    public static void sortByVersion(List<ContainerizedService> containers) {
        containers.sort((o1, o2) -> {
            String v1 = o1.getVersion();
            String v2 = o2.getVersion();
            if (v1.equals(v2)) return 0; // Short circuit for the easy case

            String[] n1 = v1.split("\\D");
            String[] n2 = v2.split("\\D");
            for (int i = 0, j = 0; i < n1.length && j < n2.length; i++, j++) {
                Integer i1 = new Integer(n1[i]);
                Integer i2 = new Integer(n2[j]);
                if (i1 > i2) {
                    return 1;
                }
                else if (i1 < i2) {
                    return -1;
                }
            }

            // Sort things like 1.0-BETA before 1.0
            boolean o1HasDash = v1.indexOf('-')>0;
            boolean o2HasDash = v2.indexOf('-')>0;
            if (o1HasDash && !o2HasDash) {
                return -1;
            }
            else if (!o1HasDash && o2HasDash) {
                return 1;
            }

            // Sort things like 1.0 before 1.0.1
            if (n1.length > n2.length) return 1;
            if (n1.length < n2.length) return -1;

            // Can't compare these versions
            return v1.compareTo(v2);
        });
    }

}

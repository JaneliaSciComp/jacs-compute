package org.janelia.jacs2.asyncservice.common;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janelia.model.service.JacsServiceData;

import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Defines a service directory.
 */
public class JacsServiceFolder {
    public static final String SERVICE_CONFIG_DIR = "sge_config";
    public static final String SERVICE_OUTPUT_DIR = "sge_output";
    public static final String SERVICE_ERROR_DIR = "sge_error";

    private final Path serviceSpecificFolderName;
    private final Path sharedFolderName;
    private final JacsServiceData serviceData;

    public JacsServiceFolder(Path serviceSpecificFolderName, Path sharedFolderName, JacsServiceData serviceData) {
        this.serviceSpecificFolderName = serviceSpecificFolderName;
        this.sharedFolderName = sharedFolderName;
        this.serviceData = serviceData;
        Preconditions.checkArgument((serviceSpecificFolderName != null || sharedFolderName != null) && getServiceSuffix() != null);
    }

    public JacsServiceData getServiceData() {
        return serviceData;
    }

    public String getServiceScriptName(String suffix) {
        return serviceData.getName() + "Cmd" + StringUtils.defaultIfBlank(suffix, "") + ".sh";
    }

    public String getServiceConfigPattern(String suffix) {
        return serviceData.getName() + "Configuration" + StringUtils.defaultIfBlank(suffix, "");
    }

    public String getServiceOutputPattern(String suffix) {
        return serviceData.getName() + "Output"  + StringUtils.defaultIfBlank(suffix, "");
    }

    public String getServiceErrorPattern(String suffix) {
        return serviceData.getName() + "Error" + StringUtils.defaultIfBlank(suffix, "");
    }

    public String getServiceSuffix() {
        if (serviceData.hasId()) {
            return serviceData.getId().toString();
        } else if (serviceData.hasParentServiceId()) {
            return serviceData.getParentServiceId().toString();
        } else {
            return null;
        }
    }

    public Path getServiceFolder(String ...more) {
        Optional<String> morePathComponents = Stream.of(more).filter(StringUtils::isNotBlank).reduce((pc1, pc2) -> pc1 + "/" + pc2);
        Path serviceFolder;
        if (serviceSpecificFolderName != null) {
            serviceFolder = serviceSpecificFolderName;
        } else {
            if (serviceData.getId() != null) {
                serviceFolder = sharedFolderName.resolve(serviceData.getId().toString());
            } else if (serviceData.getParentServiceId() != null) {
                serviceFolder = sharedFolderName.resolve(serviceData.getParentServiceId().toString());
            } else {
                serviceFolder = sharedFolderName;
            }
        }
        return morePathComponents
                .map(serviceFolder::resolve)
                .orElse(serviceFolder);
    }
}

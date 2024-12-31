package org.janelia.jacs2.auth;

import org.apache.commons.lang3.StringUtils;
import org.janelia.model.domain.enums.SubjectRole;
import org.janelia.model.service.JacsServiceData;

import jakarta.ws.rs.core.SecurityContext;

public class JacsServiceAccessDataUtils {
    public static boolean canServiceBeAccessedBy(JacsServiceData serviceData, SecurityContext securityContext) {
        return StringUtils.isBlank(serviceData.getOwnerKey()) ||
                securityContext.isUserInRole(SubjectRole.Admin.getRole()) ||
                serviceData.getOwnerKey().equals(securityContext.getUserPrincipal().getName());
    }

    public static boolean canServiceBeModifiedBy(JacsServiceData serviceData, SecurityContext securityContext) {
        return StringUtils.isBlank(serviceData.getOwnerKey()) ||
                securityContext.isUserInRole(SubjectRole.Admin.getRole()) ||
                serviceData.getOwnerKey().equals(securityContext.getUserPrincipal().getName());
    }
}

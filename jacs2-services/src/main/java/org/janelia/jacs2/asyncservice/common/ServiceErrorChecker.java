package org.janelia.jacs2.asyncservice.common;

import org.janelia.model.service.JacsServiceData;

import java.util.List;

public interface ServiceErrorChecker {
    List<String> collectErrors(JacsServiceData jacsServiceData);
}

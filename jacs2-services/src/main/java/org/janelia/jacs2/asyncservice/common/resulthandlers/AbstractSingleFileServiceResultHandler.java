package org.janelia.jacs2.asyncservice.common.resulthandlers;

import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.model.service.JacsServiceData;

import java.io.File;

public abstract class AbstractSingleFileServiceResultHandler implements ServiceResultHandler<File> {

    @Override
    public void updateServiceDataResult(JacsServiceData jacsServiceData, File result) {
        jacsServiceData.setSerializableResult(ServiceDataUtils.fileToSerializableObject(result));
    }

    @Override
    public File getServiceDataResult(JacsServiceData jacsServiceData) {
        return ServiceDataUtils.serializableObjectToFile(jacsServiceData.getSerializableResult());
    }
}

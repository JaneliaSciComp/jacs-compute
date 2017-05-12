package org.janelia.jacs2.asyncservice.common.resulthandlers;

import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;

import java.io.File;
import java.util.List;

public abstract class AbstractFileListServiceResultHandler implements ServiceResultHandler<List<File>> {

    @Override
    public void updateServiceDataResult(JacsServiceData jacsServiceData, List<File> result) {
        jacsServiceData.setSerializableResult(ServiceDataUtils.fileListToSerializableObject(result));
    }

    @Override
    public List<File> getServiceDataResult(JacsServiceData jacsServiceData) {
        return ServiceDataUtils.serializableObjectToFileList(jacsServiceData.getSerializableResult());
    }

}

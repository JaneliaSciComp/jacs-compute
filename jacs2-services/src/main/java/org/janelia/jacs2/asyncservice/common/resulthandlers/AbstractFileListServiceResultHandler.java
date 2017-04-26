package org.janelia.jacs2.asyncservice.common.resulthandlers;

import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;

import java.io.File;
import java.util.List;
import java.util.Optional;

public abstract class AbstractFileListServiceResultHandler implements ServiceResultHandler<List<File>> {

    @Override
    public void updateServiceDataResult(JacsServiceData jacsServiceData, List<File> result) {
        jacsServiceData.setStringifiedResult(ServiceDataUtils.fileListToString(result));
    }

    @Override
    public List<File> getServiceDataResult(JacsServiceData jacsServiceData) {
        return ServiceDataUtils.stringToFileList(jacsServiceData.getStringifiedResult());
    }

    @Override
    public Optional<List<File>> getExpectedServiceResult(JacsServiceData jacsServiceData) {
        return Optional.empty();
    }
}

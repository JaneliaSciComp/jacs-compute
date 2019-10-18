package org.janelia.jacs2.asyncservice;

import org.janelia.jacs2.data.NamedData;
import org.janelia.model.jacs2.DataInterval;
import org.janelia.model.jacs2.page.PageRequest;
import org.janelia.model.jacs2.page.PageResult;
import org.janelia.model.service.JacsServiceData;

import java.io.InputStream;
import java.util.Date;
import java.util.function.Supplier;
import java.util.stream.Stream;

public interface JacsServiceDataManager {
    JacsServiceData retrieveServiceById(Number instanceId);
    long countServices(JacsServiceData ref, DataInterval<Date> creationInterval);
    PageResult<JacsServiceData> searchServices(JacsServiceData ref, DataInterval<Date> creationInterval, PageRequest pageRequest);
    long getServiceStdOutputSize(JacsServiceData serviceData);
    long getServiceStdErrorSize(JacsServiceData serviceData);
    Stream<Supplier<NamedData<InputStream>>> streamServiceStdOutput(JacsServiceData serviceData);
    Stream<Supplier<NamedData<InputStream>>> streamServiceStdError(JacsServiceData serviceData);
    JacsServiceData updateService(Number instanceId, JacsServiceData serviceData);
}

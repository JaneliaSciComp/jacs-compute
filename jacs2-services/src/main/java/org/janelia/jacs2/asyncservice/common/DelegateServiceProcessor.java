package org.janelia.jacs2.asyncservice.common;

import java.util.List;
import java.util.Map;

import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ServiceMetaData;

/**
 * DelegatedServiceProcessor is a replace in place computation where the current computation
 * is delegated to the enclosed processor without persisting new service data.
 * @param <S>
 * @param <T>
 */
@MdcContext
public class DelegateServiceProcessor<S extends ServiceProcessor<T>, T> implements ServiceProcessor<T> {

    public interface ArgumentsMapper {
        List<ServiceArg> mapServiceData(JacsServiceData jacsServiceData);
    }

    public interface ServiceResourcesMapper {
        Map<String, String> mapResources(JacsServiceData jacsServiceData);
    }

    private final S delegateProcessor;
    private final ArgumentsMapper delegateArgsMapper;
    private final ServiceResourcesMapper delegateResourcesMapper;

    public DelegateServiceProcessor(S delegateProcessor,
                                    ArgumentsMapper delegateArgsMapper) {
        this.delegateProcessor = delegateProcessor;
        this.delegateArgsMapper = delegateArgsMapper;
        this.delegateResourcesMapper = JacsServiceData::getResources;
    }

    public DelegateServiceProcessor(S delegateProcessor,
                                    ArgumentsMapper delegateArgsMapper,
                                    ServiceResourcesMapper delegateResourcesMapper) {
        this.delegateProcessor = delegateProcessor;
        this.delegateArgsMapper = delegateArgsMapper;
        this.delegateResourcesMapper = delegateResourcesMapper;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return delegateProcessor.getMetadata();
    }

    @Override
    public JacsServiceData createServiceData(ServiceExecutionContext executionContext, List<ServiceArg> args) {
        JacsServiceData serviceData = delegateProcessor.createServiceData(executionContext, args);
        if (executionContext.hasServiceId()) {
            serviceData.setId(executionContext.getServiceId());
        }
        return serviceData;
    }

    @Override
    public ServiceResultHandler<T> getResultHandler() {
        return delegateProcessor.getResultHandler();
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return delegateProcessor.getErrorChecker();
    }

    @Override
    public ServiceComputation<JacsServiceResult<T>> process(JacsServiceData jacsServiceData) {
        return delegateProcessor.process(createDelegateServiceData(jacsServiceData));
    }

    private JacsServiceData createDelegateServiceData(JacsServiceData jacsServiceData) {
        return delegateProcessor.createServiceData(new ServiceExecutionContext.Builder(jacsServiceData)
                        .withId(jacsServiceData.getId())
                        .setServiceName(jacsServiceData.getName())
                        .state(JacsServiceState.RUNNING)
                        .addResources(delegateResourcesMapper.mapResources(jacsServiceData))
                        .build(),
                delegateArgsMapper.mapServiceData(jacsServiceData)
        );
    }

}

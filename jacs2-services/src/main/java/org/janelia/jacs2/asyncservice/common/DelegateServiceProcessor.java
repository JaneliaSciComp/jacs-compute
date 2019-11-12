package org.janelia.jacs2.asyncservice.common;

import java.util.List;

import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ServiceMetaData;

@MdcContext
public class DelegateServiceProcessor<S extends ServiceProcessor<T>, T> implements ServiceProcessor<T> {

    public interface ArgumentsMapper {
        List<ServiceArg> mapServiceData(JacsServiceData jacsServiceData);
    }

    private final S delegateProcessor;
    private final ArgumentsMapper delegateArgsMapper;


    public DelegateServiceProcessor(S delegateProcessor,
                                    ArgumentsMapper delegateArgsMapper) {
        this.delegateProcessor = delegateProcessor;
        this.delegateArgsMapper = delegateArgsMapper;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return delegateProcessor.getMetadata();
    }

    @Override
    public JacsServiceData createServiceData(ServiceExecutionContext executionContext, List<ServiceArg> args) {
        return delegateProcessor.createServiceData(executionContext, args);
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
                        .setServiceName(jacsServiceData.getName())
                        .state(JacsServiceState.RUNNING).build(),
                delegateArgsMapper.mapServiceData(jacsServiceData)
        );
    }

}

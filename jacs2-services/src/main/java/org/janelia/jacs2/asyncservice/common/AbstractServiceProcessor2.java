package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.asyncservice.common.resulthandlers.EmptyServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.asyncservice.utils.X11Utils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.security.util.SubjectUtils;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.lang.annotation.Annotation;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

/**
 * Base class for workflow services.
 *
 * @param <U>
 */
@MdcContext
public abstract class AbstractServiceProcessor2<U> implements ServiceProcessor<U> {

    @Inject
    protected Logger logger;

    @Inject
    @PropertyValue(name = "service.DefaultWorkingDir")
    protected String defaultWorkingDir;

    @Inject
    protected ServiceComputationFactory computationFactory;

    @Inject
    protected JacsServiceDataPersistence jacsServiceDataPersistence;


    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(getServiceClass());
    }

    @Override
    public JacsServiceData createServiceData(ServiceExecutionContext executionContext, List<ServiceArg> args) {
        ServiceMetaData smd = getMetadata();
        JacsServiceDataBuilder jacsServiceDataBuilder =
                new JacsServiceDataBuilder(executionContext.getParentServiceData())
                        .setDescription(executionContext.getDescription());
        if (executionContext.getServiceName() != null) {
            jacsServiceDataBuilder.setName(executionContext.getServiceName());
        } else {
            jacsServiceDataBuilder.setName(smd.getServiceName());
        }
        if (executionContext.getInterceptors() != null) {
            jacsServiceDataBuilder.setInterceptors(executionContext.getInterceptors());
        }
        if (executionContext.getProcessingLocation() != null) {
            jacsServiceDataBuilder.setProcessingLocation(executionContext.getProcessingLocation());
        } else {
            jacsServiceDataBuilder.setProcessingLocation(executionContext.getParentServiceData().getProcessingLocation());
        }
        jacsServiceDataBuilder.setWorkflowId(executionContext.getWorkflowId());
        jacsServiceDataBuilder.setTaskId(executionContext.getTaskId());
        if (StringUtils.isNotBlank(executionContext.getWorkspace())) {
            jacsServiceDataBuilder.setWorkspace(executionContext.getWorkspace());
        } else if (StringUtils.isNotBlank(executionContext.getParentWorkspace())) {
            jacsServiceDataBuilder.setWorkspace(executionContext.getParentWorkspace());
        }
        jacsServiceDataBuilder.addArgs(args.stream().flatMap(arg -> Stream.of(arg.toStringArray())).toArray(String[]::new));
        jacsServiceDataBuilder.setDictionaryArgs(executionContext.getDictionaryArgs());
        if (executionContext.getServiceState() != null) {
            jacsServiceDataBuilder.setState(executionContext.getServiceState());
        }
        jacsServiceDataBuilder.copyResourcesFrom(executionContext.getParentServiceData().getResources());
        jacsServiceDataBuilder.copyResourcesFrom(executionContext.getResources());
        executionContext.getWaitFor().forEach(jacsServiceDataBuilder::addDependency);
        executionContext.getWaitForIds().forEach(jacsServiceDataBuilder::addDependencyId);
        jacsServiceDataBuilder.registerProcessingNotification(executionContext.getProcessingNotification());
        jacsServiceDataBuilder.registerProcessingStageNotifications(executionContext.getProcessingStageNotifications());
        return jacsServiceDataBuilder.build();
    }

    @Override
    public ServiceResultHandler<U> getResultHandler() {
        return new EmptyServiceResultHandler<>();
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return new DefaultServiceErrorChecker(logger);
    }

//    protected List<String> getErrors(JacsServiceData jacsServiceData) {
//        return this.getErrorChecker().collectErrors(jacsServiceData);
//    }

//    /**
//     * Helper method that can be used in service processor implementations that expect dictionary arguments.
//     * @param jacsServiceData
//     * @return
//     */
//    protected String getServiceDictionaryArgsAsJson(JacsServiceData jacsServiceData) {
//        return ServiceDataUtils.serializeObjectAsJson(jacsServiceData.getDictionaryArgs());
//    }

    protected Path getServiceFolder(JacsServiceData jacsServiceData) {
        return getWorkingDirectory(jacsServiceData).getServiceFolder();
    }

    protected JacsServiceFolder getWorkingDirectory(JacsServiceData jacsServiceData) {
        if (StringUtils.isNotBlank(jacsServiceData.getWorkspace())) {
            return new JacsServiceFolder(null, Paths.get(jacsServiceData.getWorkspace()), jacsServiceData);
        } else if (StringUtils.isNotBlank(defaultWorkingDir)) {
            return new JacsServiceFolder(getServicePath(defaultWorkingDir, jacsServiceData), null, jacsServiceData);
        } else {
            return new JacsServiceFolder(getServicePath(System.getProperty("java.io.tmpdir"), jacsServiceData), null, jacsServiceData);
        }
    }

    private Path getServicePath(String baseDir, JacsServiceData jacsServiceData) {
        ImmutableList.Builder<String> pathElemsBuilder = ImmutableList.builder();
        if (StringUtils.isNotBlank(jacsServiceData.getOwnerKey())) {
            String name = SubjectUtils.getSubjectName(jacsServiceData.getOwnerKey());
            pathElemsBuilder.add(name);
        }
        pathElemsBuilder.add(jacsServiceData.getName());
        if (jacsServiceData.hasId()) {
            pathElemsBuilder.addAll(FileUtils.getTreePathComponentsForId(jacsServiceData.getId()));
        }
        return Paths.get(baseDir, pathElemsBuilder.build().toArray(new String[0])).toAbsolutePath();
    }

//    protected JacsServiceData submitDependencyIfNotFound(JacsServiceData dependency) {
//        return jacsServiceDataPersistence.createServiceIfNotFound(dependency);
//    }
//
//    protected <S> ContinuationCond<S> suspendCondition(JacsServiceData jacsServiceData) {
//        return new WaitingForDependenciesContinuationCond<>(
//                new ServiceDependenciesCompletedContinuationCond(dependenciesGetterFunc(), jacsServiceDataPersistence, logger),
//                (S state) -> jacsServiceData,
//                (S state, JacsServiceData tmpSd) -> state,
//                jacsServiceDataPersistence,
//                logger
//        ).negate();
//    }
//

    protected JacsServiceResult<U> updateServiceResult(JacsServiceData jacsServiceData, U result) {
        jacsServiceData.setSerializableResult(result);
        jacsServiceDataPersistence.updateServiceResult(jacsServiceData);
        return new JacsServiceResult<>(jacsServiceData, result);
    }

//    void verifyAndFailIfTimeOut(JacsServiceData jacsServiceData) {
//        long timeSinceStart = System.currentTimeMillis() - jacsServiceData.getProcessStartTime().getTime();
//        if (jacsServiceData.timeout() > 0 && timeSinceStart > jacsServiceData.timeout()) {
//            jacsServiceDataPersistence.updateServiceState(
//                    jacsServiceData,
//                    JacsServiceState.TIMEOUT,
//                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.TIMEOUT, String.format("Service timed out after %s ms", timeSinceStart)));
//            logger.warn("Service {} timed out after {}ms", jacsServiceData, timeSinceStart);
//            throw new ComputationException(jacsServiceData, "Service " + jacsServiceData.getId() + " timed out");
//        }
//    }

//    @Override
//    public ServiceComputation<JacsServiceResult<U>> process(JacsServiceData jacsServiceData) {
//        Path serviceFolder = getServiceFolder(jacsServiceData);
//        ServiceInput serviceInput = getServiceClass().getAnnotation(ServiceInput.class);
//        T input = (T)jacsServiceData.getDictionaryArgs().get(serviceInput.name());
//        return process(jacsServiceData, serviceFolder, input);
//    }

    protected <T> WrappedServiceProcessor<ServiceProcessor<T>, T> inline(ServiceProcessor<T> serviceProcessor) {
        return new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, serviceProcessor);
    }

    private <C extends Class<AbstractServiceProcessor2<U>>> C getServiceClass() {

        Class<?> clazz = this.getClass();
        Annotation annotation = clazz.getAnnotation(Named.class);
        while (annotation == null && clazz != null) {
            clazz = clazz.getSuperclass();
            annotation = clazz.getAnnotation(Named.class);
        }

        if (annotation==null) {
            throw new IllegalStateException("Cannot find @ServiceInput annotation in service class hierarchy");
        }

        return (C)clazz;
    }

    // Convenience method
    protected ServiceExecutionContext getContext(JacsServiceData jacsServiceData, String desc) {
        return new ServiceExecutionContext.Builder(jacsServiceData)
                .description(desc)
                .build();
    }

    // Convenience method
    protected <T> ContinuationCond.Cond<T> continueWhenTrue(boolean value, T result) {
        return new ContinuationCond.Cond<>(result, value);
    }

    // Convenience method
    protected int getTimeoutInSeconds(JacsServiceData sd) {
        long timeoutInMillis = sd.timeout();
        if (timeoutInMillis > 0) {
            return (int) timeoutInMillis / 1000;
        } else {
            return X11Utils.DEFAULT_TIMEOUT_SECONDS;
        }
    }

    // Convenience method
    protected String[] getJacsServiceArgsArray(JacsServiceData jacsServiceData) {
        if (jacsServiceData.getActualArgs() == null) {
            new ServiceArgsHandler(jacsServiceDataPersistence).updateServiceArgs(getMetadata(), jacsServiceData);
        }
        return jacsServiceData.getActualArgs().toArray(new String[jacsServiceData.getActualArgs().size()]);
    }

}

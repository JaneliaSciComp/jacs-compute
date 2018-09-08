package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.asyncservice.common.resulthandlers.EmptyServiceResultHandler;
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

    @SuppressWarnings("unchecked")
    protected <T> T getServiceInput(JacsServiceData sd, String name) {
        return (T)sd.getDictionaryArgs().get(name);
    }

    @SuppressWarnings("unchecked")
    protected <T> T getRequiredServiceInput(JacsServiceData sd, String name) {
        T value = (T)sd.getDictionaryArgs().get(name);
        if (value==null) {
            throw new IllegalStateException("Service input "+name+" must be specified");
        }
        return value;
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

    protected JacsServiceResult<U> updateServiceResult(JacsServiceData jacsServiceData, U result) {
        jacsServiceData.setSerializableResult(result);
        jacsServiceDataPersistence.updateServiceResult(jacsServiceData);
        return new JacsServiceResult<>(jacsServiceData, result);
    }

    protected <T> WrappedServiceProcessor<ServiceProcessor<T>, T> inline(ServiceProcessor<T> serviceProcessor) {
        return new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, serviceProcessor);
    }

    // Convenience method
    private <C extends Class<AbstractServiceProcessor2<U>>> C getServiceClass() {

        Class<?> clazz = this.getClass();
        Annotation annotation = clazz.getAnnotation(Named.class);
        while (annotation == null && clazz != null) {
            clazz = clazz.getSuperclass();
            annotation = clazz.getAnnotation(Named.class);
        }

        if (annotation==null) {
            throw new IllegalStateException("Cannot find @Named annotation in service class hierarchy");
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

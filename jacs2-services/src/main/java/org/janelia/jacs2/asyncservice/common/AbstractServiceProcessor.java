package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractEmptyServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.jacs2.EntityFieldValueHandler;
import org.janelia.model.security.util.SubjectUtils;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@MdcContext
public abstract class AbstractServiceProcessor<R> implements ServiceProcessor<R> {

    protected final ServiceComputationFactory computationFactory;
    protected final JacsServiceDataPersistence jacsServiceDataPersistence;
    protected final String defaultWorkingDir;
    protected final Logger logger;

    public AbstractServiceProcessor(ServiceComputationFactory computationFactory,
                                    JacsServiceDataPersistence jacsServiceDataPersistence,
                                    String defaultWorkingDir,
                                    Logger logger) {
        this.computationFactory = computationFactory;
        this.jacsServiceDataPersistence = jacsServiceDataPersistence;
        this.defaultWorkingDir = defaultWorkingDir;
        this.logger = logger;
    }

    protected <T> ContinuationCond.Cond<T> continueWhenTrue(boolean value, T result) {
        return new ContinuationCond.Cond<>(result, value);
    }

    @Override
    public JacsServiceData createServiceData(ServiceExecutionContext executionContext, List<ServiceArg> args) {
        ServiceMetaData smd = getMetadata();
        List<String> serviceArgs = args.stream().flatMap(arg -> Stream.of(arg.toStringArray())).collect(Collectors.toList());
        return createServiceData(smd.getServiceName(), executionContext, serviceArgs);
    }

    JacsServiceData createServiceData(String defaultService, ServiceExecutionContext executionContext, List<String> serviceArgs) {
        JacsServiceDataBuilder jacsServiceDataBuilder =
                new JacsServiceDataBuilder(executionContext.getParentService())
                        .setDescription(executionContext.getDescription());
        jacsServiceDataBuilder.setName(executionContext.getServiceName(defaultService));
        jacsServiceDataBuilder.setProcessingLocation(executionContext.getProcessingLocation());
        jacsServiceDataBuilder.setWorkspace(executionContext.getWorkspace());
        jacsServiceDataBuilder.addArgs(serviceArgs);
        jacsServiceDataBuilder.setDictionaryArgs(executionContext.getDictionaryArgs());
        if (executionContext.getServiceState() != null) {
            jacsServiceDataBuilder.setState(executionContext.getServiceState());
        }
        jacsServiceDataBuilder.setServiceTimeout(executionContext.getServiceTimeoutInMillis());
        jacsServiceDataBuilder.addResources(executionContext.getResourcesFromParent());
        jacsServiceDataBuilder.addResources(executionContext.getResources());
        executionContext.getWaitFor().forEach(jacsServiceDataBuilder::addDependency);
        executionContext.getWaitForIds().forEach(jacsServiceDataBuilder::addDependencyId);
        jacsServiceDataBuilder.registerProcessingNotification(executionContext.getProcessingNotification());
        jacsServiceDataBuilder.registerProcessingStageNotifications(executionContext.getProcessingStageNotifications());
        return jacsServiceDataBuilder.build();
    }

    @Override
    public ServiceResultHandler<R> getResultHandler() {
        return new AbstractEmptyServiceResultHandler<R>() {
            @Override
            public boolean isResultReady(JacsServiceData jacsServiceData) {
                return areAllDependenciesDone(jacsServiceData);
            }
        };
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return new DefaultServiceErrorChecker(logger);
    }

    List<String> getErrors(JacsServiceData jacsServiceData) {
        return this.getErrorChecker().collectErrors(jacsServiceData);
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

    protected void prepareDir(String dirName) {
        if (StringUtils.isNotBlank(dirName)) {
            Path dirPath = Paths.get(dirName);
            try {
                Files.createDirectories(dirPath);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    protected void updateOutputAndErrorPaths(JacsServiceData jacsServiceData) {
        Map<String, EntityFieldValueHandler<?>> serviceUpdates = new LinkedHashMap<>();
        JacsServiceFolder jacsServiceFolder = getWorkingDirectory(jacsServiceData);
        if (StringUtils.isBlank(jacsServiceData.getOutputPath())) {
            serviceUpdates.putAll(jacsServiceData.updateOutputPath(jacsServiceFolder.getServiceFolder(JacsServiceFolder.SERVICE_OUTPUT_DIR).toString()));
        }
        if (StringUtils.isBlank(jacsServiceData.getErrorPath())) {
            serviceUpdates.putAll(jacsServiceData.updateErrorPath(jacsServiceFolder.getServiceFolder(JacsServiceFolder.SERVICE_ERROR_DIR).toString()));
        }
        jacsServiceDataPersistence.update(jacsServiceData, serviceUpdates);
    }

    /**
     *
     * @param jacsServiceData
     * @return
     */
    protected String[] getJacsServiceArgsArray(JacsServiceData jacsServiceData) {
        if (jacsServiceData.getActualArgs() == null) {
            new ServiceArgsHandler(jacsServiceDataPersistence).updateServiceArgs(getMetadata(), jacsServiceData);
        }
        return jacsServiceData.getActualArgs().toArray(new String[jacsServiceData.getActualArgs().size()]);
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

    protected JacsServiceData submitDependencyIfNotFound(JacsServiceData dependency) {
        return jacsServiceDataPersistence.createServiceIfNotFound(dependency);
    }

    protected <S> ContinuationCond<S> suspendCondition(JacsServiceData jacsServiceData) {
        return new WaitingForDependenciesContinuationCond<>(
                new ServiceDependenciesCompletedContinuationCond(dependenciesGetterFunc(), jacsServiceDataPersistence, logger),
                (S state) -> jacsServiceData,
                (S state, JacsServiceData tmpSd) -> state,
                jacsServiceDataPersistence,
                logger
        ).negate();
    }

    protected Function<JacsServiceData, Stream<JacsServiceData>> dependenciesGetterFunc() {
        return (JacsServiceData serviceData) -> jacsServiceDataPersistence.findDirectServiceDependencies(serviceData).stream();
    }

    protected boolean areAllDependenciesDone(JacsServiceData jacsServiceData) {
        return new ServiceDependenciesCompletedContinuationCond(
                dependenciesGetterFunc(),
                jacsServiceDataPersistence,
                logger).checkCond(jacsServiceData).isCondValue();
    }

    protected JacsServiceResult<R> updateServiceResult(JacsServiceData jacsServiceData, R result) {
        this.getResultHandler().updateServiceDataResult(jacsServiceData, result);
        jacsServiceDataPersistence.updateServiceResult(jacsServiceData);
        return new JacsServiceResult<>(jacsServiceData, result);
    }

    void verifyAndFailIfTimeOut(JacsServiceData jacsServiceData) {
        long timeSinceStart = System.currentTimeMillis() - jacsServiceData.getProcessStartTime().getTime();
        if (jacsServiceData.timeoutInMillis() > 0 && timeSinceStart > jacsServiceData.timeoutInMillis()) {
            jacsServiceDataPersistence.updateServiceState(
                    jacsServiceData,
                    JacsServiceState.TIMEOUT,
                    JacsServiceData.createServiceEvent(JacsServiceEventTypes.TIMEOUT, String.format("Service timed out after %s ms", timeSinceStart)));
            logger.warn("Service {} timed out after {}ms", jacsServiceData, timeSinceStart);
            throw new ComputationException(jacsServiceData, "Service " + jacsServiceData.getId() + " timed out");
        }
    }
}

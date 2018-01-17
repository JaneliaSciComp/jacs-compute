package org.janelia.jacs2.asyncservice.common;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.asyncservice.common.resulthandlers.EmptyServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.ExprEvalHelper;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.security.util.SubjectUtils;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceDataBuilder;
import org.janelia.model.service.JacsServiceEventTypes;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
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
        return new ContinuationCond.Cond(result, value);
    }

    @Override
    public JacsServiceData createServiceData(ServiceExecutionContext executionContext, ServiceArg... args) {
        ServiceMetaData smd = getMetadata();
        JacsServiceDataBuilder jacsServiceDataBuilder =
                new JacsServiceDataBuilder(executionContext.getParentServiceData())
                        .setDescription(executionContext.getDescription());
        if (executionContext.getServiceName() != null) {
            jacsServiceDataBuilder.setName(executionContext.getServiceName());
        } else {
            jacsServiceDataBuilder.setName(smd.getServiceName());
        }
        if (executionContext.getProcessingLocation() != null) {
            jacsServiceDataBuilder.setProcessingLocation(executionContext.getProcessingLocation());
        } else {
            jacsServiceDataBuilder.setProcessingLocation(executionContext.getParentServiceData().getProcessingLocation());
        }
        if (StringUtils.isNotBlank(executionContext.getWorkspace())) {
            jacsServiceDataBuilder.setWorkspace(executionContext.getWorkspace());
        } else if (StringUtils.isNotBlank(executionContext.getParentWorkspace())) {
            jacsServiceDataBuilder.setWorkspace(executionContext.getParentWorkspace());
        }
        jacsServiceDataBuilder.addArg(Stream.of(args).flatMap(arg -> Stream.of(arg.toStringArray())).toArray(String[]::new));
        if (executionContext.getServiceState() != null) {
            jacsServiceDataBuilder.setState(executionContext.getServiceState());
        }
        if (StringUtils.isNotBlank(executionContext.getOutputPath())) {
            jacsServiceDataBuilder.setOutputPath(executionContext.getOutputPath());
        }
        if (StringUtils.isNotBlank(executionContext.getErrorPath())) {
            jacsServiceDataBuilder.setErrorPath(executionContext.getErrorPath());
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
    public ServiceResultHandler<R> getResultHandler() {
        return new EmptyServiceResultHandler<R>();
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return new DefaultServiceErrorChecker(logger);
    }

    protected List<String> getErrors(JacsServiceData jacsServiceData) {
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

    protected String[] getJacsServiceArgsArray(JacsServiceData jacsServiceData) {
        if (jacsServiceData.getActualArgs() == null) {
            // the forwarded arguments are of the form: |>${fieldname} where fieldname is a field name from the result.
            Predicate<String> isForwardedArg = arg -> arg != null && arg.startsWith("|>");
            boolean forwardedArgumentsFound = jacsServiceData.getArgs().stream().anyMatch(isForwardedArg);
            List<String> actualServiceArgs;
            if (!forwardedArgumentsFound) {
                actualServiceArgs = ImmutableList.copyOf(jacsServiceData.getArgs());
            } else {
                List<JacsServiceData> serviceDependencies = jacsServiceDataPersistence.findServiceDependencies(jacsServiceData);
                List<Object> serviceDependenciesResults = serviceDependencies.stream()
                        .filter(sd -> sd.getSerializableResult() != null)
                        .map(sd -> sd.getSerializableResult())
                        .collect(Collectors.toList());
                actualServiceArgs = jacsServiceData.getArgs().stream().map(arg -> {
                    if (isForwardedArg.test(arg)) {
                        return ExprEvalHelper.eval(arg.substring(2), serviceDependenciesResults);
                    } else {
                        return arg;
                    }
                }).collect(Collectors.toList());
            }
            jacsServiceData.setActualArgs(actualServiceArgs);
        }
        return jacsServiceData.getActualArgs().toArray(new String[jacsServiceData.getActualArgs().size()]);
    }

    private Path getServicePath(String baseDir, JacsServiceData jacsServiceData) {
        ImmutableList.Builder<String> pathElemsBuilder = ImmutableList.builder();
        if (StringUtils.isNotBlank(jacsServiceData.getOwner())) {
            String name = SubjectUtils.getSubjectName(jacsServiceData.getOwner());
            pathElemsBuilder.add(name);
        }
        pathElemsBuilder.add(jacsServiceData.getName());
        if (jacsServiceData.hasId()) {
            pathElemsBuilder.addAll(FileUtils.getTreePathComponentsForId(jacsServiceData.getId()));
        }
//        pathElemsBuilder.addAll(Arrays.asList(more));
        return Paths.get(baseDir, pathElemsBuilder.build().toArray(new String[0])).toAbsolutePath();
    }

    protected JacsServiceData submitDependencyIfNotFound(JacsServiceData dependency) {
        return jacsServiceDataPersistence.createServiceIfNotFound(dependency);
    }

    protected <S> ContinuationCond<S> suspendCondition(JacsServiceData jacsServiceData) {
        return new SuspendServiceContinuationCond<>(
                new ServiceDependenciesCompletedContinuationCond(dependenciesGetterFunc(), jacsServiceDataPersistence, logger),
                (S state) -> jacsServiceData,
                (S state, JacsServiceData tmpSd) -> state,
                jacsServiceDataPersistence,
                logger
        ).negate();
    }

    protected Function<JacsServiceData, Stream<JacsServiceData>> dependenciesGetterFunc() {
        return (JacsServiceData serviceData) -> jacsServiceDataPersistence.findServiceDependencies(serviceData).stream();
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

    protected void verifyAndFailIfTimeOut(JacsServiceData jacsServiceData) {
        long timeSinceStart = System.currentTimeMillis() - jacsServiceData.getProcessStartTime().getTime();
        if (jacsServiceData.timeout() > 0 && timeSinceStart > jacsServiceData.timeout()) {
            jacsServiceDataPersistence.updateServiceState(
                    jacsServiceData,
                    JacsServiceState.TIMEOUT,
                    Optional.of(JacsServiceData.createServiceEvent(JacsServiceEventTypes.TIMEOUT, String.format("Service timed out after %s ms", timeSinceStart))));
            logger.warn("Service {} timed out after {}ms", jacsServiceData, timeSinceStart);
            throw new ComputationException(jacsServiceData, "Service " + jacsServiceData.getId() + " timed out");
        }
    }
}

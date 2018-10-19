package org.janelia.jacs2.asyncservice.common;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.jacs2.asyncservice.common.mdc.MdcContext;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This is a generic async service wrapper which can be used to wrap other service processing.
 * This service is not available in the service registry, therefore it cannot be invoked from the outside.
 */
@MdcContext
public class GenericAsyncServiceProcessor extends AbstractServiceProcessor<Void> {

    static class ProcessingArgs extends ServiceArgs {
        @Parameter(names = "-serviceName", description = "Service name")
        String serviceName;
        @Parameter(names = "-serviceArgs", description = "Service arguments", splitter = ServiceArgs.ServiceArgSplitter.class)
        List<String> serviceArgs = new ArrayList<>();
    }

    @Inject
    public GenericAsyncServiceProcessor(ServiceComputationFactory computationFactory,
                                        JacsServiceDataPersistence jacsServiceDataPersistence,
                                        @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                        Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.createMetadata("genericAsyncService", new ProcessingArgs());
    }

    @Override
    public JacsServiceData createServiceData(ServiceExecutionContext executionContext, List<ServiceArg> args) {
        Pair<String, List<String>> serviceWithArgs = args.stream()
                .reduce(ImmutablePair.of(null, ImmutableList.of()),
                        (Pair<String, List<String>> sa, ServiceArg a) -> {
                            if ("-serviceName".equals(a.getFlag())) {
                                return ImmutablePair.of(a.getValues().stream().findFirst().orElse(sa.getLeft()), sa.getRight());
                            } else if ("-serviceArgs".equals(a.getFlag())) {
                                return ImmutablePair.of(
                                        sa.getLeft(),
                                        Stream.concat(
                                                sa.getRight().stream(),
                                                a.getValues().stream()
                                                        .flatMap(av -> new ServiceArgs.ServiceArgSplitter().split(av).stream()))
                                                .collect(Collectors.toList()));
                            } else {
                                return ImmutablePair.of(sa.getLeft(), Stream.concat(sa.getRight().stream(), Arrays.stream(a.toStringArray())).collect(Collectors.toList()));
                            }
                        },
                        (sa1, sa2) -> ImmutablePair.of(StringUtils.defaultIfBlank(sa2.getLeft(), sa1.getLeft()),
                                Stream.concat(sa1.getRight().stream(), sa2.getRight().stream()).collect(Collectors.toList())));
        return createServiceData(serviceWithArgs.getLeft(), executionContext, serviceWithArgs.getRight());
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        JacsServiceData submittedService = submit(jacsServiceData);
        PeriodicallyCheckableState<JacsServiceData> submittedServiceStateCheck = new PeriodicallyCheckableState<>(submittedService, ProcessorHelper.getSoftJobDurationLimitInSeconds(jacsServiceData.getResources()) / 100);
        return computationFactory.newCompletedComputation(submittedServiceStateCheck)
                .thenSuspendUntil((PeriodicallyCheckableState<JacsServiceData> sdState) -> new ContinuationCond.Cond<>(sdState, sdState.updateCheckTime() && isDone(sdState)))
                .thenApply((PeriodicallyCheckableState<JacsServiceData> sdState) -> getResult(sdState))
                ;
    }

    private JacsServiceData submit(JacsServiceData jacsServiceData) {
        return jacsServiceDataPersistence.createServiceIfNotFound(jacsServiceData);
    }

    private boolean isDone(PeriodicallyCheckableState<JacsServiceData> jacsServiceDataState) {
        JacsServiceData refreshServiceData = jacsServiceDataPersistence.findById(jacsServiceDataState.getState().getId());
        return refreshServiceData.hasCompleted();
    }

    private JacsServiceResult<Void> getResult(PeriodicallyCheckableState<JacsServiceData> jacsServiceDataState) {
        JacsServiceData refreshServiceData = jacsServiceDataPersistence.findById(jacsServiceDataState.getState().getId());
        if (refreshServiceData.hasCompletedSuccessfully()) {
            return new JacsServiceResult<>(refreshServiceData);
        } else {
            throw new ComputationException(refreshServiceData, "Service " + refreshServiceData.toString() + " completed unsuccessfully");
        }
    }

    private ProcessingArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new ProcessingArgs());
    }
}

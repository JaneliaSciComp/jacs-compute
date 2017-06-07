package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentProcessor;
import org.janelia.jacs2.asyncservice.alignservices.AlignmentResultFiles;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.JacsDefault;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.model.jacsservice.JacsServiceEventTypes;
import org.janelia.jacs2.model.jacsservice.JacsServiceState;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Optional;
import java.util.function.Function;

@Named("updateAlignmentResults")
public class UpdateAlignmentResultsProcessor extends AbstractBasicLifeCycleServiceProcessor<AlignmentResultFiles, AlignmentResultFiles> {

    static class UpdateAlignmentResultsArgs extends ServiceArgs {
        @Parameter(names = "-sampleId", description = "Sample ID", required = true)
        Long sampleId;
        @Parameter(names = "-objective",
                description = "Optional sample objective. If specified it retrieves all sample image files, otherwise it only retrieves the ones for the given objective",
                required = false)
        String sampleObjective;
        @Parameter(names = "-area",
                description = "Optional sample area. If specified it filters images by the specified area",
                required = false)
        String sampleArea;
        @Parameter(names = "-runId", description = "ID of the run for which to set the alignment results", required = false)
        Long runId;
        @Parameter(names = "-alignmentServiceId", description = "Alignment service ID", required = true)
        Long alignmentServiceId;
    }

    private final SampleDataService sampleDataService;
    private final AlignmentProcessor alignmentProcessor;
    private final TimebasedIdentifierGenerator idGenerator;

    @Inject
    UpdateAlignmentResultsProcessor(ServiceComputationFactory computationFactory,
                                    JacsServiceDataPersistence jacsServiceDataPersistence,
                                    @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                    SampleDataService sampleDataService,
                                    AlignmentProcessor alignmentProcessor,
                                    @JacsDefault TimebasedIdentifierGenerator idGenerator,
                                    Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.alignmentProcessor = alignmentProcessor;
        this.idGenerator = idGenerator;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(UpdateAlignmentResultsProcessor.class, new UpdateAlignmentResultsArgs());
    }

    @Override
    public ServiceResultHandler<AlignmentResultFiles> getResultHandler() {
        return new AbstractAnyServiceResultHandler<AlignmentResultFiles>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @SuppressWarnings("unchecked")
            @Override
            public AlignmentResultFiles collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<AlignmentResultFiles> intermediateResult = (JacsServiceResult<AlignmentResultFiles>)depResults;
                return intermediateResult.getResult();
            }

            @Override
            public AlignmentResultFiles getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<AlignmentResultFiles>() {});
            }
        };
    }

    @Override
    protected JacsServiceResult<AlignmentResultFiles> submitServiceDependencies(JacsServiceData jacsServiceData) {
        return new JacsServiceResult<>(jacsServiceData);
    }

    @Override
    protected ServiceComputation<JacsServiceResult<AlignmentResultFiles>> processing(JacsServiceResult<AlignmentResultFiles> depResults) {
        UpdateAlignmentResultsArgs args = getArgs(depResults.getJacsServiceData());
        return computationFactory.newCompletedComputation(depResults)
                .thenSuspendUntil(pd -> new ContinuationCond.Cond<>(pd, !suspendUntilAllDependenciesComplete(pd.getJacsServiceData())))
                .thenApply(pdCond -> {
                    JacsServiceResult<AlignmentResultFiles> pd = pdCond.getState();
                    JacsServiceData alignmentService = jacsServiceDataPersistence.findById(args.alignmentServiceId);
                    AlignmentResultFiles alignmentResultFiles = alignmentProcessor.getResultHandler().getServiceDataResult(alignmentService);
                    pd.setResult(alignmentResultFiles);
                    // !!!!!!!!!!!!!!!!! TODO
                    return pd;
                });
    }

    protected Function<JacsServiceData, JacsServiceResult<Boolean>> areAllDependenciesDoneFunc() {
        return sdp -> {
            UpdateAlignmentResultsArgs args = getArgs(sdp);
            JacsServiceData alignmentService = jacsServiceDataPersistence.findById(args.alignmentServiceId);
            if (alignmentService.hasCompletedUnsuccessfully()) {
                jacsServiceDataPersistence.updateServiceState(
                        sdp,
                        JacsServiceState.CANCELED,
                        Optional.of(JacsServiceData.createServiceEvent(
                                JacsServiceEventTypes.CANCELED,
                                String.format("Canceled because service %d finished unsuccessfully", alignmentService.getId()))));
                logger.warn("Service {} canceled because of {}", sdp, alignmentService);
                throw new ComputationException(sdp, "Service " + sdp.getId() + " canceled");
            } else if (alignmentService.hasCompletedSuccessfully()) {
                return new JacsServiceResult<>(sdp, true);
            }
            verifyAndFailIfTimeOut(sdp);
            return new JacsServiceResult<>(sdp, false);
        };
    }

    private UpdateAlignmentResultsArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(jacsServiceData.getArgsArray(), new UpdateAlignmentResultsArgs());
    }

}

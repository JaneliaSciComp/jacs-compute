package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import org.janelia.model.jacs2.domain.sample.Sample;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.VoidServiceResultHandler;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.dataservice.DomainObjectService;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.JacsServiceEvent;
import org.janelia.model.service.JacsServiceState;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Optional;

@Named("unlockSample")
public class UnlockSampleProcessor extends AbstractServiceProcessor<Void> {

    static class UnlockSampleArgs extends ServiceArgs {
        @Parameter(names = "-sampleId", description = "Sample ID", required = true)
        Long sampleId;
        @Parameter(names = "-lock", description = "Lock key", required = true)
        String lockKey;
    }

    private final SampleDataService sampleDataService;
    private final DomainObjectService domainObjectService;

    @Inject
    UnlockSampleProcessor(ServiceComputationFactory computationFactory,
                          JacsServiceDataPersistence jacsServiceDataPersistence,
                          @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                          SampleDataService sampleDataService,
                          DomainObjectService domainObjectService,
                          Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.domainObjectService = domainObjectService;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(UnlockSampleProcessor.class, new UnlockSampleArgs());
    }

    @Override
    public ServiceResultHandler<Void> getResultHandler() {
        return new VoidServiceResultHandler();
    }

    @Override
    public ServiceComputation<JacsServiceResult<Void>> process(JacsServiceData jacsServiceData) {
        UnlockSampleArgs args = getArgs(jacsServiceData);

        Sample sample = sampleDataService.getSampleById(null, args.sampleId);
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenSuspendUntil(sd -> {
                    boolean result = domainObjectService.unlock(args.lockKey, sample);
                    if (!result) {
                        if (jacsServiceData.hasNotBeenWaitingForDependencies()) {
                            // if the service has not completed yet and it's not already suspended - update the state to suspended
                            jacsServiceDataPersistence.updateServiceState(jacsServiceData, JacsServiceState.WAITING_FOR_DEPENDENCIES, JacsServiceEvent.NO_EVENT);
                        }
                        return new ContinuationCond.Cond<> (sd, false);
                    }
                    return new ContinuationCond.Cond<> (sd, true);
                })
                .thenApply(sd -> new JacsServiceResult<>(sd));
    }

    private UnlockSampleArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new UnlockSampleArgs());
    }

}

package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.model.jacs2.domain.sample.Sample;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.DataHolder;
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

@Named("lockSample")
public class LockSampleProcessor extends AbstractServiceProcessor<String> {

    static class LockSampleArgs extends ServiceArgs {
        @Parameter(names = "-sampleId", description = "Sample ID", required = true)
        Long sampleId;
    }

    private final SampleDataService sampleDataService;
    private final DomainObjectService domainObjectService;

    @Inject
    LockSampleProcessor(ServiceComputationFactory computationFactory,
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
        return ServiceArgs.getMetadata(LockSampleProcessor.class, new LockSampleArgs());
    }

    @Override
    public ServiceResultHandler<String> getResultHandler() {
        return new ServiceResultHandler<String>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return true;
            }

            @Override
            public String collectResult(JacsServiceResult<?> depResults) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void updateServiceDataResult(JacsServiceData jacsServiceData, String result) {
                jacsServiceData.setSerializableResult(result);
            }

            @Override
            public String getServiceDataResult(JacsServiceData jacsServiceData) {
                return (String) jacsServiceData.getSerializableResult();
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<String>> process(JacsServiceData jacsServiceData) {
        LockSampleArgs args = getArgs(jacsServiceData);

        DataHolder<String> lockHolder = new DataHolder<>();
        Sample sample = sampleDataService.getSampleById(null, args.sampleId);
        return computationFactory.newCompletedComputation(jacsServiceData)
                .thenSuspendUntil(sd -> {
                    String lockKey = domainObjectService.tryLock(sample);
                    if (StringUtils.isBlank(lockKey)) {
                        if (jacsServiceData.hasNotBeenWaitingForDependencies()) {
                            // if the service has not completed yet and it's not already suspended - update the state to suspended
                            jacsServiceDataPersistence.updateServiceState(jacsServiceData, JacsServiceState.WAITING_FOR_DEPENDENCIES, JacsServiceEvent.NO_EVENT);
                        }
                        return new ContinuationCond.Cond<>(sd, false);
                    }
                    lockHolder.setData(lockKey);
                    return new ContinuationCond.Cond<> (sd, true);
                })
                .thenApply(sd -> {
                    String lockKey = lockHolder.getData();
                    this.getResultHandler().updateServiceDataResult(sd, lockKey);
                    jacsServiceDataPersistence.updateServiceResult(sd);
                    return new JacsServiceResult<>(sd, lockKey);
                });
    }

    private LockSampleArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new LockSampleArgs());
    }

}

package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.janelia.it.jacs.model.domain.sample.ObjectiveSample;
import org.janelia.it.jacs.model.domain.sample.SamplePipelineRun;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
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
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;

@Named("initSampleResults")
public class InitializeSamplePipelineResultsProcessor extends AbstractBasicLifeCycleServiceProcessor<List<SamplePipelineRun>, List<SamplePipelineRun>> {

    static class InitSampleResultsArgs extends SampleServiceArgs {
        @Parameter(names = "-sampleResultsName", description = "The name for the sample results", required = false)
        String sampleResultsName;
        @Parameter(names = "-sampleProcessName", description = "The name for the sample process", required = false)
        String sampleProcessName;
    }

    private final SampleDataService sampleDataService;
    private final TimebasedIdentifierGenerator idGenerator;

    @Inject
    InitializeSamplePipelineResultsProcessor(ServiceComputationFactory computationFactory,
                                             JacsServiceDataPersistence jacsServiceDataPersistence,
                                             @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                             SampleDataService sampleDataService,
                                             @JacsDefault TimebasedIdentifierGenerator idGenerator,
                                             Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.idGenerator = idGenerator;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(InitializeSamplePipelineResultsProcessor.class, new InitSampleResultsArgs());
    }

    @Override
    public ServiceResultHandler<List<SamplePipelineRun>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<SamplePipelineRun>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @SuppressWarnings("unchecked")
            @Override
            public List<SamplePipelineRun> collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<List<SamplePipelineRun>> intermediateResult = (JacsServiceResult<List<SamplePipelineRun>>)depResults;
                return intermediateResult.getResult();
            }

            @Override
            public List<SamplePipelineRun> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SamplePipelineRun>>() {});
            }
        };
    }

    @Override
    protected JacsServiceResult<List<SamplePipelineRun>> submitServiceDependencies(JacsServiceData jacsServiceData) {
        return new JacsServiceResult<>(jacsServiceData, new ArrayList<>());
    }

    @Override
    protected ServiceComputation<JacsServiceResult<List<SamplePipelineRun>>> processing(JacsServiceResult<List<SamplePipelineRun>> depResults) {
        JacsServiceData jacsServiceData = depResults.getJacsServiceData();
        InitSampleResultsArgs args = getArgs(jacsServiceData);
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    List<ObjectiveSample> sampleObjectives = sampleDataService.getObjectivesBySampleIdAndObjective(jacsServiceData.getOwner(), args.sampleId, args.sampleObjective);
                    sampleObjectives.forEach((ObjectiveSample objectiveSample) -> {
                        SamplePipelineRun pipelineRun = new SamplePipelineRun();
                        if (args.sampleResultsId != null) {
                            pipelineRun.setId(args.sampleResultsId);
                        } else {
                            pipelineRun.setId(idGenerator.generateId());
                        }
                        pipelineRun.setPipelineProcess(args.sampleProcessName);
                        StringBuilder pipelineRunNameBuilder = new StringBuilder();
                        if (StringUtils.isNotBlank(args.sampleResultsName)) {
                            pipelineRunNameBuilder.append(args.sampleResultsName).append(' ');
                        }
                        if (pipelineRunNameBuilder.indexOf(objectiveSample.getObjective()) == -1) {
                            pipelineRunNameBuilder.append(objectiveSample.getObjective())
                                    .append(" Results");
                        }
                        pipelineRun.setName(pipelineRunNameBuilder.toString());
                        sampleDataService.addSampleObjectivePipelineRun(objectiveSample.getParent(), objectiveSample.getObjective(), pipelineRun);
                        pd.getResult().add(pipelineRun);
                    });
                    return pd;
                });
    }

    private InitSampleResultsArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new InitSampleResultsArgs());
    }

}

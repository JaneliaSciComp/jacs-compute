package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.it.jacs.model.domain.sample.LSMSummaryResult;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Named("updateSampleSummaryResults")
public class UpdateSampleSummaryResultsProcessor extends AbstractBasicLifeCycleServiceProcessor<List<LSMSummaryResult>, List<LSMSummaryResult>> {

    static class UpdateSampleSummaryResultsArgs extends ServiceArgs {
        @Parameter(names = "-sampleResultsId", description = "Sample run Id to receive the results", required = true)
        Long sampleResultsId;
        @Parameter(names = "-sampleSummaryId", description = "Sample sumnmary processing service ID", required = true)
        Long sampleSummaryId;
    }

    private final SampleDataService sampleDataService;
    private final SampleLSMSummaryProcessor sampleLSMSummaryProcessor;
    private final TimebasedIdentifierGenerator idGenerator;

    @Inject
    UpdateSampleSummaryResultsProcessor(ServiceComputationFactory computationFactory,
                                        JacsServiceDataPersistence jacsServiceDataPersistence,
                                        @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                        SampleDataService sampleDataService,
                                        SampleLSMSummaryProcessor sampleLSMSummaryProcessor,
                                        @JacsDefault TimebasedIdentifierGenerator idGenerator,
                                        Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.sampleDataService = sampleDataService;
        this.sampleLSMSummaryProcessor = sampleLSMSummaryProcessor;
        this.idGenerator = idGenerator;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(UpdateSampleSummaryResultsProcessor.class, new UpdateSampleSummaryResultsArgs());
    }

    @Override
    public ServiceResultHandler<List<LSMSummaryResult>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<LSMSummaryResult>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @SuppressWarnings("unchecked")
            @Override
            public List<LSMSummaryResult> collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<List<LSMSummaryResult>> intermediateResult = (JacsServiceResult<List<LSMSummaryResult>>)depResults;
                return intermediateResult.getResult();
            }

            @Override
            public List<LSMSummaryResult> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<LSMSummaryResult>>() {});
            }
        };
    }

    @Override
    protected JacsServiceResult<List<LSMSummaryResult>> submitServiceDependencies(JacsServiceData jacsServiceData) {
        return new JacsServiceResult<>(jacsServiceData, new ArrayList<>());
    }

    @Override
    protected ServiceComputation<JacsServiceResult<List<LSMSummaryResult>>> processing(JacsServiceResult<List<LSMSummaryResult>> depResults) {
        UpdateSampleSummaryResultsArgs args = getArgs(depResults.getJacsServiceData());
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    JacsServiceData sampleSummaryService = jacsServiceDataPersistence.findById(args.sampleSummaryId);
                    List<LSMSummary> lsmSummaryResults = sampleLSMSummaryProcessor.getResultHandler().getServiceDataResult(sampleSummaryService);
                    Multimap<Pair<Number, String>, LSMSummary> lsmSummaryResultsPerObjective = Multimaps.index(lsmSummaryResults, lsms -> ImmutablePair.of(lsms.getSampleImageFile().getId(), lsms.getSampleImageFile().getObjective()));
                    lsmSummaryResultsPerObjective.asMap().forEach((Pair<Number, String> sampleObjective, Collection<LSMSummary> objectiveLsmSummaries) -> {
                        Sample sample = sampleDataService.getSampleById(pd.getJacsServiceData().getOwner(), sampleObjective.getLeft());

                        LSMSummaryResult  sampleSummaryResult = new LSMSummaryResult();
                        sampleSummaryResult.setName("LSM Summary Results");
                        sampleSummaryResult.setId(idGenerator.generateId());
                        List<String> mips = objectiveLsmSummaries.stream().flatMap(lsmSummary -> lsmSummary.getMips().stream()).collect(Collectors.toList());
                        Optional<String> filePath = FileUtils.commonPath(mips);
                        if (filePath.isPresent()) {
                            sampleSummaryResult.setFilepath(filePath.get());
                            sampleSummaryResult.setGroups(SampleServicesUtils.createFileGroups(filePath.get(), mips));
                        } else {
                            sampleSummaryResult.setGroups(SampleServicesUtils.createFileGroups("", mips));
                        }
                        sampleDataService.addSampleObjectivePipelineRunResult(sample, sampleObjective.getRight(), args.sampleResultsId, null, sampleSummaryResult);
                        pd.getResult().add(sampleSummaryResult);
                    });
                    return pd;
                });
    }

    private UpdateSampleSummaryResultsArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(jacsServiceData.getArgsArray(), new UpdateSampleSummaryResultsArgs());
    }
}

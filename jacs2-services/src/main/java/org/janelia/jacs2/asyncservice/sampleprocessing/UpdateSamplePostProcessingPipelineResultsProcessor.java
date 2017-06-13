package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.janelia.it.jacs.model.domain.sample.Sample;
import org.janelia.it.jacs.model.domain.sample.SamplePostProcessingResult;
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
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

@Named("updateSamplePostProcessingResults")
public class UpdateSamplePostProcessingPipelineResultsProcessor extends AbstractBasicLifeCycleServiceProcessor<SamplePostProcessingResult, SamplePostProcessingResult> {

    static class UpdateSamplePostProcessingResultsArgs extends SampleServiceArgs {
        @Parameter(names = "-runId", description = "Sample pipeline run ID", required = true)
        Long sampleProcessingId;
        @Parameter(names = "-resultRootDir", description = "Stitching service ID", required = false)
        String resultsRootDir;
        @Parameter(names = "-resultDirs", description = "Stitching service ID", required = false)
        List<String> resultsDirs;
    }

    private final SampleDataService sampleDataService;
    private final TimebasedIdentifierGenerator idGenerator;

    @Inject
    UpdateSamplePostProcessingPipelineResultsProcessor(ServiceComputationFactory computationFactory,
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
        return ServiceArgs.getMetadata(UpdateSamplePostProcessingPipelineResultsProcessor.class, new UpdateSamplePostProcessingResultsArgs());
    }

    @Override
    public ServiceResultHandler<SamplePostProcessingResult> getResultHandler() {
        return new AbstractAnyServiceResultHandler<SamplePostProcessingResult>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @SuppressWarnings("unchecked")
            @Override
            public SamplePostProcessingResult collectResult(JacsServiceResult<?> depResults) {
                JacsServiceResult<SamplePostProcessingResult> intermediateResult = (JacsServiceResult<SamplePostProcessingResult>)depResults;
                return intermediateResult.getResult();
            }

            @Override
            public SamplePostProcessingResult getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<SamplePostProcessingResult>() {});
            }
        };
    }

    @Override
    protected ServiceComputation<JacsServiceResult<SamplePostProcessingResult>> processing(JacsServiceResult<SamplePostProcessingResult> depResults) {
        final String resultsPattern = "glob:**/*.{png,mp4}";
        UpdateSamplePostProcessingResultsArgs args = getArgs(depResults.getJacsServiceData());
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    SamplePostProcessingResult  samplePostProcessingResult = new SamplePostProcessingResult();
                    samplePostProcessingResult.setId(idGenerator.generateId());
                    samplePostProcessingResult.setFilepath(args.resultsRootDir);
                    List<String> mips = args.resultsDirs.stream()
                            .map(dirName -> Paths.get(dirName))
                            .flatMap(dir -> FileUtils.lookupFiles(dir, 1, resultsPattern))
                            .map(dir -> dir.toString())
                            .collect(Collectors.toList());
                    samplePostProcessingResult.setGroups(SampleServicesUtils.createFileGroups(args.resultsRootDir, mips));
                    Sample sample = sampleDataService.getSampleById(pd.getJacsServiceData().getOwner(), args.sampleId);
                    sampleDataService.addSampleObjectivePipelineRunResult(sample, args.sampleObjective, args.sampleProcessingId, null, samplePostProcessingResult);
                    pd.setResult(samplePostProcessingResult);
                    return pd;
                });
    }

    private UpdateSamplePostProcessingResultsArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(jacsServiceData.getArgsArray(), new UpdateSamplePostProcessingResultsArgs());
    }

}

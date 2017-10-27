package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.janelia.model.jacs2.domain.sample.Sample;
import org.janelia.model.jacs2.domain.sample.SamplePostProcessingResult;
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
import org.janelia.model.access.dao.mongo.utils.TimebasedIdentifierGenerator;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.dataservice.sample.SampleDataService;
import org.janelia.model.service.JacsServiceData;
import org.janelia.model.service.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

@Named("updateSamplePostProcessingResults")
public class UpdateSamplePostProcessingResultsProcessor extends AbstractBasicLifeCycleServiceProcessor<SamplePostProcessingResult, SamplePostProcessingResult> {

    static class UpdateSamplePostProcessingResultsArgs extends SampleServiceArgs {
        @Parameter(names = "-samplePostSubDir", description = "Sample post processing result sub directory", required = false)
        String samplePostSubDir;
        @Parameter(names = "-resultDirs", description = "list of result directories", required = false)
        List<String> resultsDirs;
    }

    private final SampleDataService sampleDataService;
    private final TimebasedIdentifierGenerator idGenerator;

    @Inject
    UpdateSamplePostProcessingResultsProcessor(ServiceComputationFactory computationFactory,
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
        return ServiceArgs.getMetadata(UpdateSamplePostProcessingResultsProcessor.class, new UpdateSamplePostProcessingResultsArgs());
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
                    String postProcessingOutput = getPostProcessingResultOutputDir(args).toString();
                    samplePostProcessingResult.setFilepath(postProcessingOutput);
                    List<String> mips = args.resultsDirs.stream()
                            .map(dirName -> Paths.get(dirName))
                            .flatMap(dir -> FileUtils.lookupFiles(dir, 1, resultsPattern))
                            .map(Path::toString)
                            .collect(Collectors.toList());
                    samplePostProcessingResult.setGroups(SampleServicesUtils.streamFileGroups(postProcessingOutput, mips).map(SampleServicesUtils::normalize).collect(Collectors.toList()));
                    Sample sample = sampleDataService.getSampleById(pd.getJacsServiceData().getOwner(), args.sampleId);
                    sampleDataService.addSampleObjectivePipelineRunResult(sample, args.sampleObjective, args.sampleResultsId, null, samplePostProcessingResult);
                    pd.setResult(samplePostProcessingResult);
                    return pd;
                });
    }

    private UpdateSamplePostProcessingResultsArgs getArgs(JacsServiceData jacsServiceData) {
        return ServiceArgs.parse(getJacsServiceArgsArray(jacsServiceData), new UpdateSamplePostProcessingResultsArgs());
    }

    private Path getPostProcessingResultOutputDir(UpdateSamplePostProcessingResultsArgs args) {
        return FileUtils.commonPath(args.resultsDirs)
                .map(pn -> Paths.get(pn))
                .orElse(Paths.get(StringUtils.defaultIfBlank(args.sampleDataRootDir, ""), StringUtils.defaultIfBlank(args.samplePostSubDir, "")));
    }
}

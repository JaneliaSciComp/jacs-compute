package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractBasicLifeCycleServiceProcessor;
import org.janelia.jacs2.asyncservice.common.DefaultServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceErrorChecker;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.BasicMIPsAndMoviesProcessor;
import org.janelia.jacs2.asyncservice.imageservices.BasicMIPsAndMoviesResult;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.model.jacsservice.JacsServiceData;
import org.janelia.jacs2.dataservice.persistence.JacsServiceDataPersistence;
import org.janelia.jacs2.asyncservice.common.ComputationException;
import org.janelia.jacs2.asyncservice.common.ServiceComputation;
import org.janelia.jacs2.asyncservice.common.ServiceComputationFactory;
import org.janelia.jacs2.model.jacsservice.ServiceMetaData;
import org.slf4j.Logger;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Named("getSampleMIPsAndMovies")
public class GetSampleMIPsAndMoviesProcessor extends AbstractBasicLifeCycleServiceProcessor<GetSampleMIPsAndMoviesProcessor.GetSampleMIPsIntermediateResult, List<SampleImageMIPsFile>> {

    static class GetSampleMIPsIntermediateResult extends GetSampleLsmsIntermediateResult {
        // sampleImageFileWithMips holds the correspondence between MIPsAndMovies service id and the corresponding results
        final Map<Number, SampleImageMIPsFile> sampleImageFileWithMips = new LinkedHashMap<>();

        GetSampleMIPsIntermediateResult(Number getSampleLsmsServiceDataId) {
            super(getSampleLsmsServiceDataId);
        }

        void addSampleImageMipsFile(Number basicMipsAndMoviesServiceId, SampleImageMIPsFile simf) {
            sampleImageFileWithMips.put(basicMipsAndMoviesServiceId, simf);
        }
    }

    static class SampleMIPsAndMoviesArgs extends SampleServiceArgs {
        @Parameter(names = "-options", description = "Options", required = false)
        String options = "mips:movies:legends:bcomp";
    }

    private final GetSampleImageFilesProcessor getSampleImageFilesProcessor;
    private final BasicMIPsAndMoviesProcessor basicMIPsAndMoviesProcessor;

    @Inject
    GetSampleMIPsAndMoviesProcessor(ServiceComputationFactory computationFactory,
                                    JacsServiceDataPersistence jacsServiceDataPersistence,
                                    @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                    GetSampleImageFilesProcessor getSampleImageFilesProcessor,
                                    BasicMIPsAndMoviesProcessor basicMIPsAndMoviesProcessor,
                                    Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.getSampleImageFilesProcessor = getSampleImageFilesProcessor;
        this.basicMIPsAndMoviesProcessor = basicMIPsAndMoviesProcessor;
    }

    @Override
    public ServiceMetaData getMetadata() {
        return ServiceArgs.getMetadata(GetSampleMIPsAndMoviesProcessor.class, new SampleMIPsAndMoviesArgs());
    }

    @Override
    public ServiceResultHandler<List<SampleImageMIPsFile>> getResultHandler() {
        return new AbstractAnyServiceResultHandler<List<SampleImageMIPsFile>>() {
            @Override
            public boolean isResultReady(JacsServiceResult<?> depResults) {
                return areAllDependenciesDone(depResults.getJacsServiceData());
            }

            @Override
            public List<SampleImageMIPsFile> collectResult(JacsServiceResult<?> depResults) {
                GetSampleMIPsIntermediateResult result = (GetSampleMIPsIntermediateResult) depResults.getResult();
                return result.sampleImageFileWithMips.entrySet().stream()
                        .map(simfEntry -> {
                            JacsServiceData basicMipsAndMoviesServiceData = jacsServiceDataPersistence.findById(simfEntry.getKey());

                            BasicMIPsAndMoviesResult basicMIPsAndMoviesResult = basicMIPsAndMoviesProcessor.getResultHandler().getServiceDataResult(basicMipsAndMoviesServiceData);
                            SampleImageMIPsFile simf = simfEntry.getValue();
                            simf.setMipsResultsDir(basicMIPsAndMoviesResult.getResultsDir());
                            basicMIPsAndMoviesResult.getFileList().stream().forEach(simf::addMipFile);
                            return simf;
                        })
                        .collect(Collectors.toList());
            }

            public List<SampleImageMIPsFile> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SampleImageMIPsFile>>() {});
            }
        };
    }

    @Override
    public ServiceErrorChecker getErrorChecker() {
        return new DefaultServiceErrorChecker(logger);
    }

    @Override
    protected JacsServiceResult<GetSampleMIPsIntermediateResult> submitServiceDependencies(JacsServiceData jacsServiceData) {
        SampleMIPsAndMoviesArgs args = getArgs(jacsServiceData);

        JacsServiceData getSampleLsmsServiceRef = getSampleImageFilesProcessor.createServiceData(new ServiceExecutionContext(jacsServiceData),
                new ServiceArg("-sampleId", args.sampleId.toString()),
                new ServiceArg("-objective", args.sampleObjective),
                new ServiceArg("-area", args.sampleArea),
                new ServiceArg("-sampleDataRootDir", args.sampleDataRootDir),
                new ServiceArg("-sampleLsmsSubDir", args.sampleLsmsSubDir)
        );
        JacsServiceData getSampleLsmsService = submitDependencyIfNotFound(getSampleLsmsServiceRef);
        return new JacsServiceResult<>(jacsServiceData, new GetSampleMIPsIntermediateResult(getSampleLsmsService.getId()));
    }

    @Override
    protected ServiceComputation<JacsServiceResult<GetSampleMIPsIntermediateResult>> processing(JacsServiceResult<GetSampleMIPsIntermediateResult> depResults) {
        return computationFactory.newCompletedComputation(depResults)
                .thenApply(pd -> {
                    SampleMIPsAndMoviesArgs args = getArgs(pd.getJacsServiceData());
                    JacsServiceData getSampleLsmsService = jacsServiceDataPersistence.findById(depResults.getResult().getChildServiceId());
                    List<SampleImageFile> sampleImageFiles = getSampleImageFilesProcessor.getResultHandler().getServiceDataResult(getSampleLsmsService);
                    sampleImageFiles.stream()
                            .forEach(sif -> {
                                String lsmImageFileName = sif.getWorkingFilePath();
                                if (!sif.isChanSpecDefined()) {
                                    throw new ComputationException(pd.getJacsServiceData(), "No channel spec for LSM " + sif.getId());
                                }
                                Path resultsDir =  getResultsDir(args, sif.getArea(), sif.getObjective(), new File(lsmImageFileName));
                                JacsServiceData basicMipMapsService = basicMIPsAndMoviesProcessor.createServiceData(new ServiceExecutionContext.Builder(depResults.getJacsServiceData())
                                                .waitFor(getSampleLsmsService)
                                                .build(),
                                        new ServiceArg("-imgFile", lsmImageFileName),
                                        new ServiceArg("-chanSpec", sif.getChanSpec()),
                                        new ServiceArg("-colorSpec", sif.getColorSpec()),
                                        new ServiceArg("-laser", null), // no laser info in the lsm
                                        new ServiceArg("-gain", null), // no gain info in the lsm
                                        new ServiceArg("-options", args.options),
                                        new ServiceArg("-resultsDir", resultsDir.toString())
                                );
                                basicMipMapsService = submitDependencyIfNotFound(basicMipMapsService);
                                SampleImageMIPsFile sampleImageMIPsFile = new SampleImageMIPsFile();
                                sampleImageMIPsFile.setSampleImageFile(sif);
                                depResults.getResult().addSampleImageMipsFile(basicMipMapsService.getId(), sampleImageMIPsFile);
                            });
                    return pd;
                });
    }

    private Path getResultsDir(SampleMIPsAndMoviesArgs args, String area, String objective, File lsmImageFile) {
        ImmutableList.Builder<String> pathCompBuilder = new ImmutableList.Builder<>();
        if (StringUtils.isNotBlank(args.sampleSummarySubDir)) {
            pathCompBuilder.add(args.sampleSummarySubDir);
        }
        if (StringUtils.isNotBlank(objective)) {
            pathCompBuilder.add(objective);
        }
        if (StringUtils.isNotBlank(area)) {
            pathCompBuilder.add(area);
        }
        pathCompBuilder.add(FileUtils.getFileNameOnly(lsmImageFile.getName()));
        ImmutableList<String> pathComps = pathCompBuilder.build();
        return Paths.get(args.sampleDataRootDir, pathComps.toArray(new String[pathComps.size()]));
    }

    private SampleMIPsAndMoviesArgs getArgs(JacsServiceData jacsServiceData) {
        return SampleServiceArgs.parse(jacsServiceData.getArgsArray(), new SampleMIPsAndMoviesArgs());
    }
}

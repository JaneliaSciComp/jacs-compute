package org.janelia.jacs2.asyncservice.sampleprocessing;

import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacs2.asyncservice.common.AbstractServiceProcessor;
import org.janelia.jacs2.asyncservice.common.ContinuationCond;
import org.janelia.jacs2.asyncservice.common.JacsServiceResult;
import org.janelia.jacs2.asyncservice.common.ServiceArg;
import org.janelia.jacs2.asyncservice.common.ServiceArgs;
import org.janelia.jacs2.asyncservice.common.ServiceDataUtils;
import org.janelia.jacs2.asyncservice.common.ServiceExecutionContext;
import org.janelia.jacs2.asyncservice.common.ServiceResultHandler;
import org.janelia.jacs2.asyncservice.common.WrappedServiceProcessor;
import org.janelia.jacs2.asyncservice.common.resulthandlers.AbstractAnyServiceResultHandler;
import org.janelia.jacs2.asyncservice.imageservices.BasicMIPsAndMoviesProcessor;
import org.janelia.jacs2.asyncservice.imageservices.FijiColor;
import org.janelia.jacs2.asyncservice.imageservices.FijiUtils;
import org.janelia.jacs2.asyncservice.imageservices.MIPsAndMoviesResult;
import org.janelia.jacs2.asyncservice.utils.FileUtils;
import org.janelia.jacs2.cdi.qualifier.PropertyValue;
import org.janelia.jacs2.model.jacsservice.RegisteredJacsNotification;
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
import java.util.List;
import java.util.stream.Collectors;

@Named("getSampleMIPsAndMovies")
public class GetSampleMIPsAndMoviesProcessor extends AbstractServiceProcessor<List<SampleImageMIPsFile>> {

    static class SampleMIPsAndMoviesArgs extends SampleServiceArgs {
        @Parameter(names = "-options", description = "Options", required = false)
        String options = "mips:movies:legends:bcomp";
    }

    private final WrappedServiceProcessor<GetSampleImageFilesProcessor, List<SampleImageFile>> getSampleImageFilesProcessor;
    private final WrappedServiceProcessor<BasicMIPsAndMoviesProcessor, MIPsAndMoviesResult> basicMIPsAndMoviesProcessor;

    @Inject
    GetSampleMIPsAndMoviesProcessor(ServiceComputationFactory computationFactory,
                                    JacsServiceDataPersistence jacsServiceDataPersistence,
                                    @PropertyValue(name = "service.DefaultWorkingDir") String defaultWorkingDir,
                                    GetSampleImageFilesProcessor getSampleImageFilesProcessor,
                                    BasicMIPsAndMoviesProcessor basicMIPsAndMoviesProcessor,
                                    Logger logger) {
        super(computationFactory, jacsServiceDataPersistence, defaultWorkingDir, logger);
        this.getSampleImageFilesProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, getSampleImageFilesProcessor);
        this.basicMIPsAndMoviesProcessor = new WrappedServiceProcessor<>(computationFactory, jacsServiceDataPersistence, basicMIPsAndMoviesProcessor);
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
                JacsServiceResult<List<SampleImageMIPsFile>> result = (JacsServiceResult<List<SampleImageMIPsFile>>)depResults;
                return result.getResult();
            }

            public List<SampleImageMIPsFile> getServiceDataResult(JacsServiceData jacsServiceData) {
                return ServiceDataUtils.serializableObjectToAny(jacsServiceData.getSerializableResult(), new TypeReference<List<SampleImageMIPsFile>>() {});
            }
        };
    }

    @Override
    public ServiceComputation<JacsServiceResult<List<SampleImageMIPsFile>>> process(JacsServiceData jacsServiceData) {
        SampleMIPsAndMoviesArgs args = getArgs(jacsServiceData);
        return getSampleImageFilesProcessor.process(
                new ServiceExecutionContext.Builder(jacsServiceData)
                        .build(),
                new ServiceArg("-sampleId", args.sampleId.toString()),
                new ServiceArg("-objective", args.sampleObjective),
                new ServiceArg("-area", args.sampleArea),
                new ServiceArg("-sampleDataRootDir", args.sampleDataRootDir),
                new ServiceArg("-sampleLsmsSubDir", args.sampleLsmsSubDir)
        ).thenCompose((JacsServiceResult<List<SampleImageFile>> sifr) -> {
            List<SampleImageFile> sampleImageFiles = sifr.getResult();
            List<ServiceComputation<?>> basicMIPsComputations = sampleImageFiles.stream()
                    .map((SampleImageFile sif) -> {
                        String lsmImageFileName = sif.getWorkingFilePath();
                        if (!sif.isChanSpecDefined()) {
                            throw new ComputationException(jacsServiceData, "No channel spec for LSM " + sif.getId());
                        }
                        Path resultsDir =  getResultsDir(args, sif.getArea(), sif.getObjective(), new File(lsmImageFileName));
                        // get color spec from the LSM
                        List<FijiColor> colors = FijiUtils.getColorSpec(sif.getColorSpec(), sif.getChanSpec());
                        if (colors.isEmpty()) {
                            colors = FijiUtils.getDefaultColorSpec(sif.getChanSpec(), "RGB", '1');
                        }
                        String colorSpec = colors.stream().map(c -> String.valueOf(c.getCode())).collect(Collectors.joining(""));
                        String divSpec = colors.stream().map(c -> String.valueOf(c.getDivisor())).collect(Collectors.joining(""));
                        return basicMIPsAndMoviesProcessor.process(
                                new ServiceExecutionContext.Builder(jacsServiceData)
                                        .waitFor(sifr.getJacsServiceData())
                                        .registerProcessingNotification(
                                                jacsServiceData.getProcessingStageNotification(FlylightSampleEvents.SUMMARY_MIPMAPS, new RegisteredJacsNotification().withDefaultLifecycleStages())
                                                        .map(n -> n.addNotificationField("sampleId", sif.getSampleId())
                                                                        .addNotificationField("lsmId", sif.getId())
                                                                        .addNotificationField("objective", sif.getObjective())
                                                                        .addNotificationField("area", sif.getArea())
                                                        )
                                        )
                                        .build(),
                                new ServiceArg("-imgFile", lsmImageFileName),
                                new ServiceArg("-chanSpec", sif.getChanSpec()),
                                new ServiceArg("-colorSpec", colorSpec),
                                new ServiceArg("-divSpec", divSpec),
                                new ServiceArg("-laser", ""), // no laser info in the lsm
                                new ServiceArg("-gain", ""), // no gain info in the lsm
                                new ServiceArg("-options", args.options),
                                new ServiceArg("-resultsDir", resultsDir.toString())
                        ).thenApply((JacsServiceResult<MIPsAndMoviesResult> mipsAndMoviesResult) -> {
                            SampleImageMIPsFile sampleImageMIPsFile = new SampleImageMIPsFile();
                            sampleImageMIPsFile.setSampleImageFile(sif);
                            sampleImageMIPsFile.setMipsResultsDir(mipsAndMoviesResult.getResult().getResultsDir());
                            mipsAndMoviesResult.getResult().getFileList().stream().forEach(sampleImageMIPsFile::addMipFile);
                            return sampleImageMIPsFile;
                        });
                    })
                    .collect(Collectors.toList());
                    ;
            return computationFactory
                    .newCompletedComputation(sampleImageFiles)
                    .thenCombineAll(basicMIPsComputations, (List<SampleImageFile> sifs, List<?> lsmsWithMIPs) -> (List<SampleImageMIPsFile>) lsmsWithMIPs);
        }).thenSuspendUntil((List<SampleImageMIPsFile> lsmsWithMIPs) -> new ContinuationCond.Cond<>(lsmsWithMIPs, !suspendUntilAllDependenciesComplete(jacsServiceData))
        ).thenApply((ContinuationCond.Cond<List<SampleImageMIPsFile>> lsmsWithMIPsCond) -> this.updateServiceResult(jacsServiceData, lsmsWithMIPsCond.getState()))
        ;
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
